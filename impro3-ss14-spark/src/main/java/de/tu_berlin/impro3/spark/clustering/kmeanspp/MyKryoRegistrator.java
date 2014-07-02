package de.tu_berlin.impro3.spark.clustering.kmeanspp;

import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;


public class MyKryoRegistrator implements KryoRegistrator {

	@Override
	public void registerClasses(Kryo kryo) {
		kryo.register(DocPoint.class);
	}
	
}