package com.example.config;

import org.apache.flink.configuration.MemorySize;

import java.time.Duration;

//PRO
//Separación de la configuración del código
//Integración con Docker y Kubernetes
//Estandarización de la configuración
//Flexibilidad en la configuracion de recursos
//Despliegue distintos entornos
//CONTRA
//Posible complejidad en entornos complejos
//Necesidad de reiniciar la aplicación
//Necesidad de documentación clara
//Posible complejidad de rastreo de cambios

public class Bootstrap {
    public final Duration rolloverInterval;
    public final Duration inactivityInterval;
    public final MemorySize maxPartSize;
    public final Duration boundedOutOfOrderness;
    public final Duration tumblingWindowSize;

    public Bootstrap() {
        Env env = new Env();
        rolloverInterval = Constants.ROLLOVER_INTERVAL;
        inactivityInterval = Constants.INACTIVITY_INTERVAL;
        maxPartSize = Constants.MAX_PART_SIZE;
        boundedOutOfOrderness = env.boundedOutOfOrderness;
        tumblingWindowSize = env.tumblingWindowSize;
    }
}
