package com.ansonator.test.config;

import org.springframework.context.annotation.Configuration;

/**
 * This is only done to avoid having our tests do component scanning and pick up beans from the application context that we do not need in
 * our tests. Is there a better way to do this?
 */
@Configuration
// @ComponentScan(excludeFilters = { @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, value = Main.class) })
public class EmptyConfig {

}
