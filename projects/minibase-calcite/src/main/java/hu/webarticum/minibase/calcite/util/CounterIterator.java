package hu.webarticum.minibase.calcite.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

import hu.webarticum.miniconnect.lang.LargeInteger;

public class CounterIterator implements Iterator<LargeInteger> {
    
    private final LargeInteger until;
    
    private LargeInteger next = LargeInteger.ZERO;
    
    
    public CounterIterator(LargeInteger until) {
        this.until = until;
    }
    

    @Override
    public boolean hasNext() {
        return next.isLessThan(until);
    }

    @Override
    public LargeInteger next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        
        LargeInteger result = next;
        next = next.increment();
        return result;
    }

}
