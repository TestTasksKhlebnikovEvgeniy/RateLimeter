package crpt;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.gridkit.nanocloud.Cloud;
import org.gridkit.nanocloud.CloudFactory;
import org.gridkit.nanocloud.VX;
import org.gridkit.vicluster.ViNode;
import org.modelmapper.ModelMapper;
import org.modelmapper.config.Configuration.AccessLevel;
import org.modelmapper.convention.MatchingStrategies;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.persistence.Column;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

@SpringBootApplication
public class CrptApplication {

	public static void main(String[] args) {
		SpringApplication.run(CrptApplication.class, args);
	}

	IgniteCache<String, BucketState> cache = Cache.createCache();
	AsyncBatchingTokenBucket asyncBatchingTokenBucket = new AsyncBatchingTokenBucket(100L, null, null, cache);

	public class BucketParams implements Serializable {

		private static final long serialVersionUID = 7791032620474627799L;
		final long capacity;
		final long nanosToGenerateToken;

		public BucketParams(long capacity, Duration period) {
			this.capacity = capacity;
			this.nanosToGenerateToken = period.toNanos() / capacity;
		}

	}

	public final class BucketState {

		long availableTokens;
		long lastRefillNanoTime;

		public BucketState(BucketParams params, long nanoTime) {
			this.lastRefillNanoTime = nanoTime;
			this.availableTokens = params.capacity;
		}

		public BucketState(BucketState other) {
			this.lastRefillNanoTime = other.lastRefillNanoTime;
			this.availableTokens = other.availableTokens;
		}

		public void refill(BucketParams params, long nanoTime) {
			long nanosSinceLastRefill = nanoTime - this.lastRefillNanoTime;
			if (nanosSinceLastRefill <= params.nanosToGenerateToken) {
				return;
			}
			long tokensSinceLastRefill = nanosSinceLastRefill / params.nanosToGenerateToken;
			availableTokens = Math.min(params.capacity, availableTokens + tokensSinceLastRefill);
			lastRefillNanoTime += tokensSinceLastRefill * params.nanosToGenerateToken;
		}

	}

	public class BatchAcquireProcessor implements Serializable, EntryProcessor<String, BucketState, List<Boolean>> {

		private static final long serialVersionUID = 188419377158359581L;

		@Override
		public List<Boolean> process(MutableEntry<String, BucketState> entry, Object... arguments)
				throws EntryProcessorException {
			final List<Long> tryConsumeCommands = (List<Long>) arguments[0];
			final BucketParams params = (BucketParams) arguments[1];
			long nanoTime = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
			BucketState state = entry.exists() ? new BucketState(entry.getValue()) : new BucketState(params, nanoTime);
			state.refill(params, nanoTime);
			List<Boolean> results = new ArrayList<>(tryConsumeCommands.size());
			long consumedTokens = 0;
			for (Long tokensToConsume : tryConsumeCommands) {
				if (state.availableTokens < tokensToConsume) {
					results.add(false);
				} else {
					state.availableTokens -= tokensToConsume;
					results.add(true);
					consumedTokens += tokensToConsume;
				}
			}
			if (consumedTokens > 0) {
				entry.setValue(state);
			}
			return results;
		}
	}

	public class MutableEntryDecorator<K> implements MutableEntry<K, BucketState> {

		private BucketState state;
		private boolean stateModified;

		public MutableEntryDecorator(BucketState state) {
			this.state = state;
		}

		@Override
		public BucketState getValue() {
			if (state == null) {
				throw new IllegalStateException("'exists' must be called before 'get'");
			}
			return state;
		}

		@Override
		public void setValue(BucketState value) {
			this.state = Objects.requireNonNull(state);
			this.stateModified = true;
		}

		public boolean isStateModified() {
			return stateModified;
		}

		@Override
		public boolean exists() {
			return state != null;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public K getKey() {
			throw new UnsupportedOperationException();
		}

		@Override
		public <T> T unwrap(Class<T> clazz) {
			throw new UnsupportedOperationException();
		}

	}

	private static class Cache {

		private static Cloud cloud;
		private static ViNode server;
		private static Ignite ignite;

		private static IgniteCache<String, BucketState> createCache() {
			// start separated JVM on current host
			cloud = CloudFactory.createCloud();
			cloud.node("**").x(VX.TYPE).setLocal();
			server = cloud.node("stateful-ignite-server");

			int serverDiscoveryPort = 47500;
			String serverNodeAdress = "localhost:" + serverDiscoveryPort;

			// start ignite server in dedicated JVM
			server.exec((Runnable & Serializable) () -> {
				TcpDiscoveryVmIpFinder neverFindOthers = new TcpDiscoveryVmIpFinder();
				neverFindOthers.setAddresses(Collections.singleton(serverNodeAdress));

				TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi();
				tcpDiscoverySpi.setIpFinder(neverFindOthers);
				tcpDiscoverySpi.setLocalPort(serverDiscoveryPort);

				IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
				igniteConfiguration.setClientMode(false);
				igniteConfiguration.setDiscoverySpi(tcpDiscoverySpi);

				CacheConfiguration cacheConfiguration = new CacheConfiguration("my_buckets");
				Ignite ignite = Ignition.start(igniteConfiguration);
				ignite.getOrCreateCache(cacheConfiguration);
			});

			// start ignite client which works inside current JVM and does not hold data
			TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
			ipFinder.setAddresses(Collections.singleton(serverNodeAdress));
			TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi();
			tcpDiscoverySpi.setIpFinder(ipFinder);

			IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
			igniteConfiguration.setDiscoverySpi(tcpDiscoverySpi);
			igniteConfiguration.setClientMode(true);
			ignite = Ignition.start(igniteConfiguration);
			CacheConfiguration cacheConfiguration = new CacheConfiguration("my_buckets");
			return ignite.getOrCreateCache(cacheConfiguration);
		}

	}

	public class AsyncBatchingTokenBucket {

		private final BucketParams bucketParams;
		private final IgniteCache<String, BucketState> cache;
		private final String key;

		private final BatchHelper<Long, Boolean, List<Long>, List<Boolean>> batchHelper = BatchHelper
				.async(this::invokeBatch);

		public AsyncBatchingTokenBucket(long permits, Duration period, String key,
				IgniteCache<String, BucketState> cache) {
			this.bucketParams = new BucketParams(permits, period);
			this.key = key;
			this.cache = cache;
		}

		public CompletableFuture<Boolean> tryAcquire(long numberTokens) {
			return batchHelper.executeAsync(numberTokens);
		}

		private CompletableFuture<List<Boolean>> invokeBatch(List<Long> commands) {
			IgniteFuture<List<Boolean>> future = cache.invokeAsync(key, new BatchAcquireProcessor(), commands,
					bucketParams);
			return convertFuture(future);
		}

		private static <T> CompletableFuture<T> convertFuture(IgniteFuture<T> igniteFuture) {
			CompletableFuture<T> completableFuture = new CompletableFuture<>();
			igniteFuture.listen((IgniteInClosure<IgniteFuture<T>>) completedIgniteFuture -> {
				try {
					completableFuture.complete(completedIgniteFuture.get());
				} catch (Throwable t) {
					completableFuture.completeExceptionally(t);
				}
			});
			return completableFuture;
		}
	}

	// TODO
	@Getter
	@EqualsAndHashCode(of = "docId")
	@NoArgsConstructor
	class DocumentModel {

		@Id
		String docId;

		@Column(name = "doc_status")
		String docStatus;
		
		@Column(name = "doc_type")
		String docType;
		
		@Column(name = "import_request")
		boolean importRequest;
		
		@Column(name = "owner_inn")
		String ownerInn;
		
		@Column(name = "participant_inn")
		String participantInn;
		
		@Column(name = "producer_inn")
		String producerInn;
		
		@Column(name = "production_date")
		Date productionDate;
		
		@Column(name = "production_Type")
		String productionType;
		
		@Column(name = "certificate_document")
		String certificateDocument;
		
		@Column(name = "certificate_document_date")
		Date certificateDocumentDate;
		
		@Column(name = "certificate_document_number")
		String certificateDocumentNumber;
		
		@Column(name = "tnved_code")
		String tnvedCode;
		
		@Column(name = "uit_code")
		String uitCode;
		
		@Column(name = "uitu_code")
		String uituCode;
		
		@Column(name = "reg_date")
		Date regDate;
		
		@Column(name = "reg_number")
		String regNumber;
	}

	@Getter
	@AllArgsConstructor
	@NoArgsConstructor
	class DocumentDto {
		
		String docId;

		String docStatus;
		String docType;
		boolean importRequest;
		String ownerInn;
		String participantInn;
		String producerInn;
		Date productionDate;
		String productionType;
		String certificateDocument;
		Date certificateDocumentDate;
		String certificateDocumentNumber;
		String tnvedCode;
		String uitCode;
		String uituCode;
		Date regDate;
		String regNumber;
	}

	@Configuration
	class CrptApiConfiguration {

		@Bean
		ModelMapper getModelMapper() {
			ModelMapper modelMapper = new ModelMapper();
			modelMapper.getConfiguration().setFieldMatchingEnabled(true).setFieldAccessLevel(AccessLevel.PRIVATE)
					.setMatchingStrategy(MatchingStrategies.STRICT);
			return modelMapper;
		}

	}

	@Service
	interface DocumentService {

		boolean addDocument(DocumentDto documentDto);

	}

	@Service
	@RequiredArgsConstructor
	class DocumentServiceImpl implements DocumentService {

		final DocumentRepository documentRepository;
		final ModelMapper modelMapper;

		@Override
		public boolean addDocument(DocumentDto documentDto) {
			if (documentRepository.existsById(documentDto.getDocId())) {
				return false;
			}
			DocumentModel document = modelMapper.map(documentDto, DocumentModel.class);
			documentRepository.save(document);
			return true;
		}
	}

	interface DocumentRepository extends JpaRepository<DocumentModel, String> {

	}

	@RestController
	@RequiredArgsConstructor
	@RequestMapping("https://ismp.crpt.ru/api/v3/lk/documents")
	class DocumentController {

		final DocumentService documentService;

		@PostMapping("create")
		public boolean addDocument(@RequestBody DocumentDto documentDto) throws InterruptedException, ExecutionException {

			if (asyncBatchingTokenBucket.tryAcquire(100L).get()) {
				return documentService.addDocument(documentDto);
			} else {
				return false;
			}
			
		}

	}

}
