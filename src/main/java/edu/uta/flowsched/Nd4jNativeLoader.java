package edu.uta.flowsched;

import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.factory.Nd4jBackend;
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
import java.util.ServiceLoader;

public class Nd4jNativeLoader {
    private static boolean loaded = false;
    private static final Logger log = LoggerFactory.getLogger(Nd4jNativeLoader.class);

    public static void load() {
        if (loaded) return;

        try {
            Path tempDir = Files.createTempDirectory("nd4j-native");
            tempDir.toFile().deleteOnExit();

            // Extract libs
            File libnd4j = extractLibrary("libnd4jcpu.so", tempDir);
            File libjni = extractLibrary("libjnind4jcpu.so", tempDir);

            // Load
            System.load(libnd4j.getAbsolutePath());
            System.load(libjni.getAbsolutePath());
            System.setProperty("org.nd4j.linalg.factory.Nd4jBackend", "org.nd4j.linalg.cpu.nativecpu.CpuBackend");
            Thread.currentThread().setContextClassLoader(Nd4j.class.getClassLoader());
            ServiceLoader<Nd4jBackend> loader = ServiceLoader.load(Nd4jBackend.class);

            for (Nd4jBackend b : loader) {
                log.info("Found backend: " + b.getClass().getName());
                log.info("Available: " + b.isAvailable());
            }
            loaded = true;
            log.info("Successfully loaded ND4J native libraries from {}", tempDir);
        } catch (Exception e) {
            log.error("Failed to load ND4J native libraries", e);
            throw new RuntimeException(e);
        }
    }

    private static File extractLibrary(String name, Path dir) throws IOException {
        Bundle bundle = FrameworkUtil.getBundle(Nd4jNativeLoader.class);
        URL resource = bundle.getResource("META-INF/native/" + name);
        if (resource == null) throw new RuntimeException("Library not found: " + name);

        File target = new File(dir.toFile(), name);
        try (InputStream in = resource.openStream(); FileOutputStream out = new FileOutputStream(target)) {
            byte[] buffer = new byte[8192];
            int len;
            while ((len = in.read(buffer)) > 0) out.write(buffer, 0, len);
        }
        target.setExecutable(true);
        return target;
    }
}