/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.operator;

import java.util.Collection;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.GenericDataSink;
import eu.stratosphere.sopremo.packages.ITypeRegistry;
import eu.stratosphere.sopremo.serialization.SopremoRecordLayout;
import eu.stratosphere.sopremo.serialization.SopremoRecordPostPass;

/**
 */
public final class PlanWithSopremoPostPass extends Plan {
	private final SopremoRecordLayout layout;

	private final ITypeRegistry typeRegistry;

	/**
	 * Initializes PlanWithSopremoPostPass.
	 * 
	 * @param sinks
	 */
	public PlanWithSopremoPostPass(final SopremoRecordLayout layout, final ITypeRegistry typeRegistry,
			final Collection<GenericDataSink> sinks) {
		super(sinks);
		if (layout == null)
			throw new NullPointerException();
		this.layout = layout;
		this.typeRegistry = typeRegistry;
	}

	/**
	 * Returns the layout.
	 * 
	 * @return the layout
	 */
	public SopremoRecordLayout getLayout() {
		return this.layout;
	}

	/**
	 * Returns the typeRegistry.
	 * 
	 * @return the typeRegistry
	 */
	public ITypeRegistry getTypeRegistry() {
		return this.typeRegistry;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.api.plan.Plan#getPostPassClassName()
	 */
	@Override
	public String getPostPassClassName() {
		return SopremoRecordPostPass.class.getName();
	}
}