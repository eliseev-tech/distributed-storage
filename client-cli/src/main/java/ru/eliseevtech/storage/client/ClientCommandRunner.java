package ru.eliseevtech.storage.client;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class ClientCommandRunner implements CommandLineRunner {

    private final StorageClient storageClient;

    public ClientCommandRunner(StorageClient storageClient) {
        this.storageClient = storageClient;
    }

    @Override
    public void run(String... args) throws Exception {
        if (args.length < 1) {
            printUsage();
            return;
        }
        String command = args[0];
        switch (command) {
            case "upload" -> handleUpload(args, false);
            case "resume-upload" -> handleUpload(args, true);
            case "download" -> handleDownload(args);
            default -> printUsage();
        }
    }

    private void handleUpload(String[] args, boolean resume) throws Exception {
        if (args.length != 3) {
            printUsage();
            return;
        }
        String remotePath = args[1];
        String localPath = args[2];
        storageClient.upload(remotePath, localPath, resume);
    }

    private void handleDownload(String[] args) throws Exception {
        if (args.length != 3) {
            printUsage();
            return;
        }
        String remotePath = args[1];
        String localPath = args[2];
        storageClient.download(remotePath, localPath);
    }

    private void printUsage() {
        System.out.println("""
                Usage:
                  upload <remotePath> <localPath>
                  resume-upload <remotePath> <localPath>
                  download <remotePath> <localPath>
                """);
    }

}
