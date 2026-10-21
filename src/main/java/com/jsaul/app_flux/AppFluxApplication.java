package com.jsaul.app_flux;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import reactor.core.publisher.Flux;

@SpringBootApplication
public class AppFluxApplication implements CommandLineRunner { // CommandLineRunner: Ejecucion en consola de Spring Boot

	private static final Logger log = LoggerFactory.getLogger(AppFluxApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(AppFluxApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		
		Flux<String> nombres = Flux.just("Andres", "Juan", "Ramon", "Pedro", "Maria", "Diego")
			.doOnNext(element -> {
				if (element.isEmpty()) {
					throw new RuntimeException("Nombres no pueden ser vacios.");
				}

				System.out.println(element);
				
			});

			nombres.subscribe(
				e -> log.info(e),
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
