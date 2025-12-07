package ru.eliseevtech.storage.client;

public class ProgressBar {

    private final long totalBytes;
    private long lastPrintedPercent = -1;

    public ProgressBar(long totalBytes) {
        this.totalBytes = totalBytes;
    }

    public void update(long currentBytes) {
        if (totalBytes <= 0) {
            return;
        }
        long percent = currentBytes * 100 / totalBytes;
        if (percent == lastPrintedPercent) {
            return;
        }
        lastPrintedPercent = percent;
        int width = 30;
        int filled = (int) (percent * width / 100);
        StringBuilder bar = new StringBuilder();
        bar.append('[');
        for (int i = 0; i < width; i++) {
            bar.append(i < filled ? '#' : '.');
        }
        bar.append(']');
        System.out.printf("\r%s %3d%%", bar, percent);
        if (percent == 100) {
            System.out.println();
        }
    }

}
