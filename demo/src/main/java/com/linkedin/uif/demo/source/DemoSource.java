package com.linkedin.uif.demo.source;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.demo.extractor.DemoExtractor;
import com.linkedin.uif.source.Source;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.source.workunit.Extract;
import com.linkedin.uif.source.workunit.WorkUnit;

/**
 * An demo implementation of {@link Source}.
 *
 * <p>
 *   This source creates one {@link com.linkedin.uif.source.workunit.WorkUnit}
 *   for each file to pull and uses the {@link DemoExtractor} to pull the data.
 * </p>
 *
 * @author ynli
 */
@SuppressWarnings("unused")
public class DemoSource implements Source<String, String> {

  private static final String SOURCE_FILE_KEY = "source.file";

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    List<WorkUnit> workUnits = Lists.newArrayList();

    if (!state.contains(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL)) {
      return workUnits;
    }

    // Create a single snapshot-type extract for all files
    Extract extract = new Extract(
        state,
        Extract.TableType.SNAPSHOT_ONLY,
        state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY, "DemoNamespace"),
        "DemoTable");

    String filesToPull = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL);
    for (String file : Splitter.on(',').omitEmptyStrings().split(filesToPull)) {
      // Create one work unit for each file to pull
      WorkUnit workUnit = new WorkUnit(state, extract);
      workUnit.setProp(SOURCE_FILE_KEY, file);
      workUnits.add(workUnit);
    }

    return workUnits;
  }

  @Override
  public Extractor<String, String> getExtractor(WorkUnitState state) throws IOException {
    return new DemoExtractor(state);
  }

  @Override
  public void shutdown(SourceState state) {
    // Nothing to do
  }
}
