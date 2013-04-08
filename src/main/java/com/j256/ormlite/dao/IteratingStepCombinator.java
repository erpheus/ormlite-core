package com.j256.ormlite.dao;

import java.util.List;

public abstract class IteratingStepCombinator<RType,SType>{
    public boolean hasToStop = false;
    public abstract RType combine(RType previous, SType stepResult);
}

class IteratingStepListCombinator<T> extends IteratingStepCombinator<List<T>,List<T>>{
    @Override
    public List<T> combine(List<T> previous, List<T> stepResult) {
        if (stepResult!=null){
            previous.addAll(stepResult);
        }
        return previous;
    }
}
