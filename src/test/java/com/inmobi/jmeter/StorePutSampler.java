package com.inmobi.jmeter;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import com.inmobi.assignment.StoreImpl;
import com.inmobi.exceptions.InitializationException;

public class StorePutSampler extends AbstractJavaSamplerClient {
    
    @Override
    public Arguments getDefaultParameters() {
    
        Arguments args = new Arguments();
        args.addArgument("Config file", "");
        args.addArgument("key", "");
        return args;
    }
    
    public SampleResult runTest(JavaSamplerContext context) {
    
        SampleResult result = new SampleResult();
        result.setSampleLabel("Put");
        result.setDataType(SampleResult.TEXT);
        result.sampleStart();
        try {
            String key = context.getParameter("key");
            StoreGetSampler.store.put(key, key.getBytes());
            result.setSuccessful(true);
            result.setResponseCodeOK();
            
        } catch (Throwable e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            result.setSuccessful(false);
            result.setResponseMessage("Unexpected exception");
            result.setResponseData(sw.toString().getBytes());
        }
        
        result.sampleEnd();
        return result;
    }
    
    @Override
    public void setupTest(JavaSamplerContext context) {
    
        if (StoreGetSampler.store == null) {
            StoreGetSampler.store = new StoreImpl<String>();
            try {
                StoreGetSampler.store.init(context.getParameter("Config file"));
            } catch (InitializationException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
    
}
