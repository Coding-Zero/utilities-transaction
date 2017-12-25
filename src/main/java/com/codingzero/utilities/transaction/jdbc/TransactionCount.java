package com.codingzero.utilities.transaction.jdbc;

class TransactionCount {

    private int starts;
    private int commits;
    private int rollbacks;

    public TransactionCount() {
        this.starts = 0;
        this.commits = 0;
        this.rollbacks = 0;
    }

    public void start() {
        starts ++;
    }

    public void commit() {
        commits ++;
        checkForInvalidCommits();
    }

    public void rollback() {
        rollbacks ++;
        checkForInvalidRollbacks();
    }

    public int getStarts() {
        return starts;
    }

    public int getCommits() {
        return commits;
    }

    public int getRollbacks() {
        return rollbacks;
    }

    private void checkForInvalidCommits() {
        if (getStarts() < getCommits()) {
            throw new IllegalArgumentException(
                    "Commits is greater than starts, (" + getStarts() + " < " + getCommits() + ")");
        }
    }

    private void checkForInvalidRollbacks() {
        if (getStarts() < getRollbacks()) {
            throw new IllegalArgumentException(
                    "Rollbacks is greater than starts, (" + getStarts() + " < " + getRollbacks() + ")");
        }
    }

    public boolean isLastCall() {
        return (getStarts() == getCommits() || getStarts() == getRollbacks());
    }

    public void reset() {
        this.starts = 0;
        this.commits = 0;
        this.rollbacks = 0;
    }

    @Override
    public String toString() {
        return "TransactionCount{" +
                "starts=" + starts +
                ", commits=" + commits +
                ", rollbacks=" + rollbacks +
                '}';
    }
}
