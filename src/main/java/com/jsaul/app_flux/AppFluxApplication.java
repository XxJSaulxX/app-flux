package com.jsaul.app_flux;

import java.time.Duration;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.jsaul.app_flux.models.Comments;
import com.jsaul.app_flux.models.User;
import com.jsaul.app_flux.models.UserComments;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SpringBootApplication
public class AppFluxApplication implements CommandLineRunner { // CommandLineRunner: Ejecucion en consola de Spring Boot

	private static final Logger log = LoggerFactory.getLogger(AppFluxApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(AppFluxApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {

		exampleContrapersion();
	}

	/**
	 * Limtar el Flujo de datos
	 */
	public void exampleContrapersion() {
		Flux.range(1, 10)
			.log() // loguea el flujo de datos
			.limitRate(5) // TODO: PRIMRA FORMA DE LIMITAR EL FLUJO
			.subscribe(/*new Subscriber<Integer>() {
				// TODO: SEGUNDA FORMA DE LIMITAR EL FLUJO
				private Subscription s;

				private Integer limit = 5;
				private Integer consumido = 0;

				@Override
				public void onSubscribe(Subscription s) {
					this.s = s;
					s.request(limit);
				}

				@Override
				public void onNext(Integer t) {
					log.info(t.toString());
					consumido++;

					if (consumido == limit) {
						consumido = 0;
						s.request(limit);
					}
				}
				@Override
				public void onError(Throwable t) {
					// TODO Auto-generated method stub
					throw new UnsupportedOperationException("Unimplemented method 'onError'");
				}

				@Override
				public void onComplete() {
					log.info("completado");
				}


			}*/);
	}

	public void exampleIntervalFormCreate() {

		Flux.create(emitter -> {
			Timer timer = new Timer();
			timer.schedule(new TimerTask() {

				private Integer count = 0;

				@Override
				public void run() {

					emitter.next(++count);

					if (count == 10) {
						timer.cancel();
						emitter.complete();
					}

					if (count == 5) {
						timer.cancel();
						emitter.error(new InterruptedException("Solo hasta 4"));
					}
				}
			}, 1000, 1000);
		})
		.subscribe(
			next -> log.info(next.toString()),
			error -> log.error(error.getMessage()),
			() -> log.info("Ha finalizado el flujo con exito.")
		);
	}

	public void exampleInfinityInterval() throws InterruptedException {

		CountDownLatch latch = new CountDownLatch(1); // ota forma de bloquear el hilo principal

		Flux.interval(Duration.ofSeconds(1))
			.doOnTerminate(() -> latch.countDown())
			.flatMap(i -> {
				if (i >= 5) {
					return Flux.error(new InterruptedException("Solo hasta 4"));
				}

				return Flux.just(i);	
			})
			.map(n -> "Hola " + n)
			//.retry(2) // reintenta 2 veces mas en caso de error
			.subscribe(s -> log.info(s), error -> log.error(error.getMessage()));

			latch.await(); // bloquea el hilo principal hasta que se termine el flujo	
	}

	/**
	 * Intervalo en el flujo
	 * @throws Exception
	 */
	public void exampleDelayElements() throws Exception {

		Flux<Integer> range = Flux.range(1, 3)
			.delayElements(Duration.ofSeconds(1))
			.doOnNext(n -> log.info(n.toString()));
		
		range.blockLast();  // TODO: solo lo usare para ver en consola el delay
		//range.subscribe();


	}


	/**
	 * Intervalo usando zipWith en el flujo
	 * @throws Exception
	 */
	public void exampleZipWithInterval() throws Exception {

		Flux<Integer> range = Flux.range(1, 12);
		Flux<Long> interval = Flux.interval(Duration.ofSeconds(2));

		range.zipWith(interval, (r, i) -> r)
			.doOnNext(n -> log.info(n.toString()))
			.blockLast();  // TODO: solo lo usare para ver en consola el delay 
			//.subscribe();

	}


	/**
	 * Combinar 2 flujos 
	 * @throws Exception
	 */
	public void exampleZipWithRange() throws Exception {

		Flux.just(1, 2, 3, 4)
			.map(n -> n*2)
			.zipWith(Flux.range(0, 4), (first, second) -> String.format("Primer Flux: %d, Segundo Flux: %d", first, second))
			.subscribe(text -> log.info(text));

	}

	/**
	 * Combinar 2 flujos de datos (Mono) en uno solo, segunda forma
	 * @throws Exception
	 */
	public void exampleUserCommentsZipWithForm2() throws Exception {

		Mono<User> userMono = Mono.fromCallable(() -> new User("Juan", "Natividad"));

		Mono<Comments> commentsMono = Mono.fromCallable(() -> {
			Comments comments = new Comments();
			comments.addComment("Comentario. 1");
			comments.addComment("Comentario. 2");
			comments.addComment("Comentario. 3	");
			return comments;
		});

		userMono
			.zipWith(commentsMono)
			.map(tuple -> {
				return new UserComments(tuple.getT1(), tuple.getT2());
			})
			.subscribe(uc -> log.info(uc.toString()));

	}

	/**
	 * Combinar 2 flujos de datos (Mono) en uno solo
	 * @throws Exception
	 */
	public void exampleUserCommentsZipWith() throws Exception {

		Mono<User> userMono = Mono.fromCallable(() -> new User("Juan", "Natividad"));

		Mono<Comments> commentsMono = Mono.fromCallable(() -> {
			Comments comments = new Comments();
			comments.addComment("Comentario. 1");
			comments.addComment("Comentario. 2");
			comments.addComment("Comentario. 3	");
			return comments;
		});

		userMono.zipWith(commentsMono, (u, c) -> new UserComments(u, c))
		.subscribe(uc -> log.info(uc.toString()));

	}

	public void exampleUserCommentsFlatMap() throws Exception {

		Mono<User> userMono = Mono.fromCallable(() -> new User("Juan", "Natividad"));

		Mono<Comments> commentsMono = Mono.fromCallable(() -> {
			Comments comments = new Comments();
			comments.addComment("Comentario. 1");
			comments.addComment("Comentario. 2");
			comments.addComment("Comentario. 3	");
			return comments;
		});

		userMono.flatMap(u -> commentsMono.map(c -> new UserComments(u, c)))
		.subscribe(uc -> log.info(uc.toString()));

	}

	public void exampleCollectList() throws Exception {

		List<User> usersList = List.of(
			new User("Juan", "Perez"), 
			new User("Andres", "Fulan"), 
			new User("Juan", "Vatividad"), 
			new User("Bruce", "Lee"), 
			new User("Bruce", "Willis")
		);
		
		Flux.fromIterable(usersList)
			.collectList()
			.subscribe(u -> log.info(u.toString()));

	}

	public void exampleToString() throws Exception {

		List<User> usersList = List.of(
			new User("Juan", "Perez"), 
			new User("Andres", "Fulan"), 
			new User("Juan", "Vatividad"), 
			new User("Bruce", "Lee"), 
			new User("Bruce", "Willis")
		);
		
		Flux.fromIterable(usersList)
			.map(user -> user.getName().toUpperCase().concat(" ").concat(user.getLastName().toUpperCase()))
			.flatMap(name -> {
				if (name.contains("bruce".toUpperCase())) {
					return Mono.just(name);
				} else {
					return Mono.empty();
				}
			})
			.map(name -> {
				return name.toLowerCase();
			})
			.subscribe(u -> log.info(u.toString()));

	}

	public void exampleFlapMap() throws Exception {

		List<String> namesList = List.of("Andres Fulan", "Juan Vatividad", "Ramon Sultan", "Pedro Lopez", "Bruce Lee", "Bruce Willis");
		
		Flux.fromIterable(namesList)
			.map(fullName -> new User(fullName.split(" ")[0].toUpperCase(), fullName.split(" ")[1].toUpperCase()))
			.flatMap(user -> {
				if (user.getName().equalsIgnoreCase("bruce")) {
					return Mono.just(user);
				} else {
					return Mono.empty();
				}
			})
			.map(u -> {
				String name = u.getName().toLowerCase();
				u.setName(name);
				return u;
			})
			.subscribe(u -> log.info(u.toString()));

	}

	public void exampleIterable() throws Exception {

		List<String> namesList = List.of("Andres Fulan", "Juan Vatividad", "Ramon Sultan", "Pedro Lopez", "Bruce Lee", "Bruce Willis");
		Flux<String> names = Flux.fromIterable(namesList);
		
		//Flux<String> names = Flux.just("Andres Fulan", "Juan Vatividad", "Ramon Sultan", "Pedro Lopez", "Bruce Lee", "Bruce Willis");

		Flux<User> users= names.map(fullName -> new User(fullName.split(" ")[0].toUpperCase(), fullName.split(" ")[1].toUpperCase()))
			.filter(user -> user.getName().equalsIgnoreCase("bruce"))
			.doOnNext(user -> {
				if (user == null) {
					throw new RuntimeException("Usuarios no pueden ser vacios.");
				}

				System.out.println(user.getName().concat(" ").concat(user.getLastName()));
				
			})
			.map(u -> {
				String name = u.getName().toLowerCase();
				u.setName(name);
				return u;
			});

		users.subscribe(
			u -> log.info(u.toString()),
			error -> log.error(error.getMessage()),
			new Runnable() {
				@Override
				public void run() {
					log.info("Ha finalizado la ejecucion del observable con exito.");
				}
			}
		);

	}

}
