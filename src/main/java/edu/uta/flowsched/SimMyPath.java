package edu.uta.flowsched;

import java.util.LinkedList;
import java.util.List;

public class SimMyPath extends MyPath {
    List<SimMyLink> simLinks;

    public SimMyPath(MyPath path) {
        super(path);
        simLinks = new LinkedList<>();
        linksNoEdge().forEach(link -> {
            long newFreeCap = ((MyLink) link).getDefaultCapacity() / ((MyLink) link).getActiveFlows();
            SimMyLink simMyLink = new SimMyLink(newFreeCap, Math.max(((MyLink) link).getActiveFlows() - 1, 0));
            simLinks.add(simMyLink);
        });
    }

    @Override
    public double getEffectiveRTT() {
        return 0.7 * super.getEffectiveRTT();
    }

    public long getCurrentFairShare() {
        return simLinks.stream()
                .mapToLong(SimMyLink::getCurrentFairShare)
                .min()
                .orElse(0);
    }

    public long getProjectedFairShare() {
        return simLinks.stream()
                .mapToLong(SimMyLink::getProjectedFairShare)
                .min()
                .orElse(0);
    }

    public long getBottleneckFreeCap() {
        return simLinks.stream()
                .mapToLong(link -> link.freeCap)
                .min()
                .orElse(0);
    }

    public double getCurrentActiveFlows() {
        return simLinks.stream()
                .mapToInt(link -> link.activeFlows)
                .max()
                .orElse(0);
    }

    static class SimMyLink {
        long freeCap;
        int activeFlows;

        public SimMyLink(long freeCap, int activeFlows) {
            this.freeCap = freeCap;
            this.activeFlows = activeFlows;
        }

        public long getProjectedFairShare() {
            return freeCap / (activeFlows + 1);
        }

        public long getCurrentFairShare() {
            return activeFlows > 0 ? freeCap / activeFlows : freeCap;
        }
    }
}