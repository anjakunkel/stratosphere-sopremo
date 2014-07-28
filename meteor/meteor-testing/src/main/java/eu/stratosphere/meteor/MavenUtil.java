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
package eu.stratosphere.meteor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.maven.cli.MavenCli;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;

/**
 */
public class MavenUtil {

	public static File buildJarForProject(final String canonicalProjectPath, final String jarName) {
		
		
		try {
			boolean success = false;
			String line;
			final Process p = Runtime.getRuntime().exec("mvn jar:jar -o -Djar.finalName=" + jarName);
			final BufferedReader bri = new BufferedReader(new InputStreamReader(p.getInputStream()));
			while ((line = bri.readLine()) != null) {
				System.out.println(line);
				// FIXME hacky
				if (line.contains("BUILD SUCCESS"))
					success = true;
			}
			bri.close();
			p.waitFor();

			if (!success)
				buildJarForProjectUsingMavenCli(canonicalProjectPath, jarName);

		} catch (final IOException err) {
			buildJarForProjectUsingMavenCli(canonicalProjectPath, jarName);

		} catch (final InterruptedException e) {
			buildJarForProjectUsingMavenCli(canonicalProjectPath, jarName);
		}
		return new File("target", jarName + ".jar");
	}

	protected static void buildJarForProjectUsingMavenCli(
			final String canonicalProjectPath, final String jarName) {
		MavenCli cli = new MavenCli();
		if (cli.doMain(
				new String[] { "jar:jar", "-Djar.finalName=" + jarName },
				new File(".").getAbsolutePath(), System.out, System.out) != 0) {
			throw new RuntimeException("Jar for desired project path "
					+ canonicalProjectPath + " could not be build.");
		}
	}

	public static String getProjectName() {
		try {
			final Reader reader = new FileReader("pom.xml");
			final MavenXpp3Reader xpp3Reader = new MavenXpp3Reader();
			final Model model = xpp3Reader.read(reader);
			reader.close();
			return model.getName();
		} catch (final Exception e) {
			throw new RuntimeException("Could not read project name", e);
		}
	}
}
