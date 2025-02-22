/*
 * Copyright 2021-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.r2dbc.documentation;

import static org.mockito.Mockito.*;
import static org.springframework.data.domain.ExampleMatcher.*;
import static org.springframework.data.domain.ExampleMatcher.GenericPropertyMatchers.endsWith;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleMatcher;
import org.springframework.data.r2dbc.repository.R2dbcRepository;

/**
 * Code to demonstrate Query By Example in reference documentation.
 *
 * @since 1.3
 * @author Greg Turnquist
 */
public class QueryByExampleTests {

	private EmployeeRepository repository;

	@Test
	void queryByExampleSimple() {

		this.repository = mock(EmployeeRepository.class);

		when(this.repository.findAll((Example<Employee>) any())) //
				.thenReturn(Flux.just( //
						new Employee(1, "Frodo", "ring bearer")));

		// tag::example[]
		Employee employee = new Employee(); // <1>
		employee.setName("Frodo");

		Example<Employee> example = Example.of(employee); // <2>

		Flux<Employee> employees = repository.findAll(example); // <3>

		// do whatever with the flux
		// end::example[]

		employees //
				.as(StepVerifier::create) //
				.expectNext(new Employee(1, "Frodo", "ring bearer")) //
				.verifyComplete();
	}

	@Test
	void queryByExampleCustomMatcher() {

		this.repository = mock(EmployeeRepository.class);

		when(this.repository.findAll((Example<Employee>) any())) //
				.thenReturn(Flux.just( //
						new Employee(1, "Frodo Baggins", "ring bearer"), //
						new Employee(1, "Bilbo Baggins", "burglar")));

		// tag::example-2[]
		Employee employee = new Employee();
		employee.setName("Baggins");
		employee.setRole("ring bearer");

		ExampleMatcher matcher = matching() // <1>
				.withMatcher("name", endsWith()) // <2>
				.withIncludeNullValues() // <3>
				.withIgnorePaths("role"); // <4>
		Example<Employee> example = Example.of(employee, matcher); // <5>

		Flux<Employee> employees = repository.findAll(example);

		// do whatever with the flux
		// end::example-2[]

		employees //
				.as(StepVerifier::create) //
				.expectNext(new Employee(1, "Frodo Baggins", "ring bearer")) //
				.expectNext(new Employee(1, "Bilbo Baggins", "burglar")) //
				.verifyComplete();
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public class Employee {

		private @Id Integer id;
		private String name;
		private String role;
	}

	public interface EmployeeRepository extends R2dbcRepository<Employee, Integer> {}
}
