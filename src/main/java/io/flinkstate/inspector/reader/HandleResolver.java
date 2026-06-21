package io.flinkstate.inspector.reader;

import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Resolves IncrementalRemoteKeyedStateHandle entries to local SST file paths.
 * Handles RelativeFileStateHandle, ByteStreamStateHandle extraction to temp files,
 * and shared state file resolution.
 */
final class HandleResolver {

    private static final Logger LOG = LoggerFactory.getLogger(HandleResolver.class);

    private HandleResolver() {
    }

    static List<String> resolveSstFiles(
            IncrementalRemoteKeyedStateHandle handle,
            String localCheckpointPath,
            List<File> tempFilesOut) throws Exception {
        List<String> paths = new ArrayList<>();
        for (HandleAndLocalPath hp : handle.getSharedState()) {
            String lp = hp.getLocalPath();
            if (lp != null && !lp.endsWith(".sst") && !isSstHandle(hp)) continue;
            String resolved = resolveHandlePath(hp.getHandle(), localCheckpointPath, tempFilesOut);
            if (resolved != null) paths.add(resolved);
        }
        for (HandleAndLocalPath hp : handle.getPrivateState()) {
            String lp = hp.getLocalPath();
            if (lp != null && !lp.endsWith(".sst") && !isSstHandle(hp)) continue;
            String resolved = resolveHandlePath(hp.getHandle(), localCheckpointPath, tempFilesOut);
            if (resolved != null) paths.add(resolved);
        }
        return paths;
    }

    private static boolean isSstHandle(HandleAndLocalPath hp) {
        StreamStateHandle h = hp.getHandle();
        if (h instanceof FileStateHandle) {
            String name = ((FileStateHandle) h).getFilePath().getName();
            return name.endsWith(".sst") || !name.contains(".");
        }
        return true;
    }

    private static String resolveHandlePath(
            StreamStateHandle handle, String localCheckpointPath,
            List<File> tempFilesOut) throws Exception {
        if (handle instanceof RelativeFileStateHandle) {
            String fileName = ((RelativeFileStateHandle) handle).getRelativePath();
            File f = new File(localCheckpointPath, fileName);
            if (f.exists()) return f.getAbsolutePath();
        } else if (handle instanceof FileStateHandle) {
            String fileName = ((FileStateHandle) handle).getFilePath().getName();
            File f = new File(localCheckpointPath, fileName);
            if (f.exists()) return f.getAbsolutePath();
            File shared = new File(localCheckpointPath, "shared/" + fileName);
            if (shared.exists()) return shared.getAbsolutePath();
        }
        try (InputStream in = handle.openInputStream()) {
            File tempFile = File.createTempFile("sst-", ".sst",
                new File(localCheckpointPath));
            tempFilesOut.add(tempFile);
            try (FileOutputStream out = new FileOutputStream(tempFile)) {
                byte[] buf = new byte[8192];
                int n;
                while ((n = in.read(buf)) != -1) out.write(buf, 0, n);
            }
            return tempFile.getAbsolutePath();
        } catch (Exception e) {
            LOG.warn("Failed to extract state handle: {}", e.getMessage());
            return null;
        }
    }
}
