package eu.stratosphere.sopremo.operator;

import java.beans.BeanDescriptor;
import java.beans.FeatureDescriptor;
import java.beans.IndexedPropertyDescriptor;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.pact.SopremoUtil;

/**
 * A composite operator may be composed of several {@link ElementaryOperator}s
 * and other CompositeOperators.<br>
 * This class should always be used as a base for new operators which would be
 * translated to more than one PACT, especially if some kind of projection or
 * selection is used.
 */
public abstract class CompositeOperator<Self extends CompositeOperator<Self>>
		extends Operator<Self> {
	/**
	 * Initializes the CompositeOperator with the number of outputs set to 1.
	 */
	public CompositeOperator() {
		super();
	}

	/**
	 * Initializes the CompositeOperator with the given number of outputs.
	 * 
	 * @param numberOfOutputs
	 *            the number of outputs
	 */
	public CompositeOperator(final int numberOfInputs, final int numberOfOutputs) {
		super(numberOfInputs, numberOfInputs, numberOfOutputs, numberOfOutputs);
	}

	/**
	 * Initializes the CompositeOperator with the given number of inputs.
	 * 
	 * @param minInputs
	 *            the minimum number of inputs
	 * @param maxInputs
	 *            the maximum number of inputs
	 */
	public CompositeOperator(final int minInputs, final int maxInputs,
			final int minOutputs, final int maxOutputs) {
		super(minInputs, maxInputs, minOutputs, maxOutputs);
	}

	public abstract void addImplementation(SopremoModule module);

	/**
	 * Returns a {@link SopremoModule} that consists entirely of
	 * {@link ElementaryOperator}s. The module can be seen as an expanded
	 * version of this operator where all CompositeOperators are recursively
	 * translated to ElementaryOperators.
	 * 
	 * @return a module of ElementaryOperators
	 */
	@Override
	public ElementarySopremoModule asElementaryOperators() {
		final SopremoModule module = new SopremoModule(this.getNumInputs(),
				this.getNumOutputs());
		module.setName(this.getName());
		this.addImplementation(module);
		this.propagateAllProperties(module);

		// inherit the CompositeOperator's DoP, if it was not changed by the
		// Operator developer explicitly.
		if (this.getDegreeOfParallelism() != Operator.STANDARD_DEGREE_OF_PARALLELISM)
			for (final Operator<?> moduleOperator : module.getReachableNodes())
				if (moduleOperator.getDegreeOfParallelism() == Operator.STANDARD_DEGREE_OF_PARALLELISM)
					moduleOperator.setDegreeOfParallelism(this
							.getDegreeOfParallelism());

		module.validate();
		return module.asElementary();
	}

	/*
	 * Find all properties of this operator that are marked to be propagated 
	 * down to it's elementary children. Propagate the values of these properties.
	 */
	public void propagateAllProperties(SopremoModule module) {
		PropertyDescriptor[] properties = this.getBeanInfo()
				.getPropertyDescriptors();

		for (PropertyDescriptor property : properties) {
			// Iterate properties. Find the ones that shall be propagated.
			
			Property propertyDescription;
			
			if(property instanceof IndexedPropertyDescriptor) {
				// That's too complicated for us, we cannot handle this.
				if ((propertyDescription = ((IndexedPropertyDescriptor) property).getIndexedWriteMethod().getAnnotation(Property.class)) != null) {
					if(propertyDescription.propagate()) {
						SopremoUtil.LOG.error("IndexedProperty ("+property.getName()+" at "+this+") is not allowed to be set to (propagate=true).");
					}
				}
				continue; 
			}
			
			if ((propertyDescription = property.getWriteMethod().getAnnotation(Property.class)) != null) {
				if(propertyDescription.propagate()) {
					//Propagate
					propagateSingleProperty(module, this, property);
				}
			}
			
		}

	}

	protected void propagateSingleProperty(	SopremoModule module, Operator<?> propagator, PropertyDescriptor property) {
		Iterable<? extends Operator<?>> allOperators = module.getReachableNodes();
		try {
			Object value = property.getReadMethod().invoke(propagator);

			for (Operator<?> op : allOperators) {

				PropertyDescriptor destProperty = findMatchingProperty(op, property);
				if (destProperty != null) {
					destProperty.getWriteMethod().invoke(op, value);
				}
			}
		} catch (IllegalAccessException e) {
			SopremoUtil.LOG.error("Error at propagation of property "+property.getName()+".", e);
		} catch (IllegalArgumentException e) {
			SopremoUtil.LOG.error("Error at propagation of property "+property.getName()+".", e);
		} catch (InvocationTargetException e) {
			SopremoUtil.LOG.error("Error at propagation of property "+property.getName()+".", e);
		}
	}

	private PropertyDescriptor findMatchingProperty( Operator<?> op, PropertyDescriptor propertyToFind) {
		for (PropertyDescriptor propertyDescriptor : op.getBeanInfo().getPropertyDescriptors()) {
			// Iterate properties. Find the ones that shall be propagated - those with the same name.
			if(propertyDescriptor.getName().equals(propertyToFind.getName())) {
				return propertyDescriptor;
			}
		}
		
		return null;
	}

	protected Info getBeanInfo() {
		return super.getBeanInfo();
	}

	public static class CompositeInfo extends Info {
		private final BeanDescriptor classDescriptor;

		private PropertyDescriptor[] properties;

		public CompositeInfo(final Class<?> clazz) {
			super(clazz);
			this.classDescriptor = new BeanDescriptor(clazz);
			this.setNames(this.classDescriptor, clazz.getAnnotation(Name.class));

			this.findProperties(clazz);
		}

		@Override
		public BeanDescriptor getBeanDescriptor() {
			return this.classDescriptor;
		}

		@Override
		public PropertyDescriptor[] getPropertyDescriptors() {
			return this.properties;
		}

		private void findProperties(final Class<?> clazz) {
			final List<PropertyDescriptor> properties = new ArrayList<PropertyDescriptor>();
			try {
				for (final PropertyDescriptor descriptor : Introspector
						.getBeanInfo(clazz, 0).getPropertyDescriptors()) {
					final Method writeMethod = descriptor instanceof IndexedPropertyDescriptor ? ((IndexedPropertyDescriptor) descriptor)
							.getIndexedWriteMethod() : descriptor
							.getWriteMethod();
					Property propertyDescription;
					if (writeMethod != null
							&& (propertyDescription = writeMethod
									.getAnnotation(Property.class)) != null) {
						descriptor.setHidden(propertyDescription.hidden());
						properties.add(descriptor);

						descriptor.setValue(INPUT, propertyDescription.input());
						this.setNames(descriptor,
								writeMethod.getAnnotation(Name.class));
					}
				}
			} catch (final IntrospectionException e) {
				e.printStackTrace();
			}
			this.properties = properties
					.toArray(new PropertyDescriptor[properties.size()]);
		}

		private void setNames(final FeatureDescriptor description,
				final Name annotation) {
			if (annotation != null)
				description.setValue(NAME, annotation);
		}
	}
}
