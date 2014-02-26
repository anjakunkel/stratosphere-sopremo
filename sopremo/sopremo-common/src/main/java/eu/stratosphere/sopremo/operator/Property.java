package eu.stratosphere.sopremo.operator;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD })
public @interface Property {
	String[] description() default {};

	boolean expert() default false;

	boolean flag() default false;

	boolean hidden() default false;

	boolean input() default false;

	boolean preferred() default false;

	/**
	 * If set to true on an CompositeOperator, 
	 * the value of this property is copied to the properties 
	 * with the same name at all contained Operators.
	 * Does nothing if set on another Operator.
	 * 
	 * Defaults to false.
	 */
	boolean propagate() default false;
}
