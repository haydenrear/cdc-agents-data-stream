package com.hayden.cdcagentsdatastream.lock;

import com.google.common.util.concurrent.Striped;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

import java.util.concurrent.locks.Lock;

@Aspect
@Component
@RequiredArgsConstructor
@Slf4j
public class StripedLockAspect {

    Striped<Lock> lockStriped = Striped.lock(100);

    @Around("@annotation(locked)")
    public Object around(ProceedingJoinPoint joinPoint, StripedLock locked) throws Throwable {
        Object[] args = joinPoint.getArgs();
        if (locked.stringArg() >= args.length) {
            throw new IllegalArgumentException("Invalid number of arguments for %s".formatted(locked.getClass().getName()));
        }
        var found = String.valueOf(args[locked.stringArg()]);

        if (found == null)
            throw new IllegalArgumentException("Invalid lock key for %s".formatted(locked.getClass().getName()));

        try {
            lockStriped.get(found).lock();

            var ret = joinPoint.proceed(args);

            return ret;
        } finally {
            lockStriped.get(found).unlock();
        }

    }
}
