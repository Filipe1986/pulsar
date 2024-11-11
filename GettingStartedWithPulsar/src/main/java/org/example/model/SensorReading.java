package org.example.model;

public class SensorReading {
    public String sensor_name;
    public float value;

    public SensorReading() {
    }

    public SensorReading (String name, float input) {
        this.sensor_name = name;
        this.value = input;
    }

    public String getSensorName() {
        return this.sensor_name;
    }

    public void setSensorName(String name) {
        this.sensor_name = name;
    }

    public float getValue() {
        return this.value;
    }

    public void setValue(float input) { this.value = input; }
}