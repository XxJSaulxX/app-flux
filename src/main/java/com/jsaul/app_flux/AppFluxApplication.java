package com.jsaul.app_flux;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.jsaul.app_flux.models.User;

import reactor.core.publisher.Flux;

@SpringBootApplication
public class AppFluxApplication implements CommandLineRunner { // CommandLineRunner: Ejecucion en consola de Spring Boot

	private static final Logger log = LoggerFactory.getLogger(AppFluxApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(AppFluxApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		
		Flux<User> users = Flux.just("Andres Fulan", "Juan Vatividad", "Ramon Sultan", "Pedro Lopez", "Bruce Lee", "Bruce Willis")
			.map(fullName -> new User(fullName.split(" ")[0].toUpperCase(), fullName.split(" ")[1].toUpperCase()))
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
