package edu.uta.flowsched;

import com.google.ortools.sat.BoolVar;
import com.google.ortools.sat.CpModel;
import org.osgi.framework.Bundle;
import org.osgi.framework.FrameworkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

public class OrToolsLoader {
    private static final Logger log = LoggerFactory.getLogger(OrToolsLoader.class);
    public static final OrToolsLoader INSTANCE = new OrToolsLoader();
    private static boolean loaded = false;
    private static Path tempDir;

    public void loadNativeLibraries() {
        if (loaded) {
            return;
        }

        try {
            // Create a temporary directory for our libraries
            tempDir = Files.createTempDirectory("ortools_libs");
            tempDir.toFile().deleteOnExit();

            // Set the java.library.path to include our temp directory
            String libraryPath = System.getProperty("java.library.path", "");
            System.setProperty("java.library.path", tempDir.toString() + File.pathSeparator + libraryPath);

            // Extract libraries
            File ortoolsLib = extractLibrary("libortools.so.9", tempDir);
            File ortoolsSymlink = new File(tempDir.toFile(), "libortools.so");
            if (!ortoolsSymlink.exists()) {
                Files.createSymbolicLink(ortoolsSymlink.toPath(), ortoolsLib.toPath());
            }

            File jniLib = extractLibrary("libjniortools.so", tempDir);

            // Load libraries in correct order
            System.load(ortoolsLib.getAbsolutePath());
            System.load(jniLib.getAbsolutePath());

            loaded = true;
            CpModel model = new CpModel();
            BoolVar var = model.newBoolVar("x");
            var.toString();
            log.info("Successfully loaded OR-Tools native libraries from {}", tempDir);
        } catch (Exception e) {
            log.error("Failed to load OR-Tools native libraries", e);
            throw new RuntimeException("Failed to load OR-Tools native libraries", e);
        }
    }

    private static File extractLibrary(String libraryName, Path tempDir) throws IOException {
        Bundle bundle = FrameworkUtil.getBundle(OrToolsLoader.class);
        URL libraryURL = bundle.getResource("META-INF/native/" + libraryName);

        if (libraryURL == null) {
            throw new RuntimeException("Native library not found in bundle: " + libraryName);
        }

        File tempFile = new File(tempDir.toFile(), libraryName);
        if (!tempFile.exists()) {
            try (InputStream in = libraryURL.openStream();
                 FileOutputStream out = new FileOutputStream(tempFile)) {
                byte[] buffer = new byte[8192];
                int length;
                while ((length = in.read(buffer)) > 0) {
                    out.write(buffer, 0, length);
                }
            }
            tempFile.setExecutable(true);
        }

        return tempFile;
    }
}