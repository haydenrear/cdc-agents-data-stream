package com.hayden.cdcagentsdatastream.trigger;

import com.google.common.util.concurrent.Striped;
import com.hayden.utilitymodule.db.DbDataSourceTrigger;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.locks.Lock;

@Aspect
@Component
@RequiredArgsConstructor
@Slf4j
public class DbTriggerAspect {

    @Autowired
    private DbDataSourceTrigger trigger;

    @Around("@annotation(locked)")
    public Object around(ProceedingJoinPoint joinPoint, DbTriggerRoute locked) {
        return trigger.doOnKey(sKey -> {
            sKey.setKey(locked.route());
            try {
                return joinPoint.proceed(joinPoint.getArgs());
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });

    }
}
