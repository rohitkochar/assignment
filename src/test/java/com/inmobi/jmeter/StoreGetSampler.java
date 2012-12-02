package com.inmobi.jmeter;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import com.inmobi.assignment.Store;
import com.inmobi.assignment.StoreImpl;
import com.inmobi.exceptions.InitializationException;

public class StoreGetSampler extends AbstractJavaSamplerClient {
    
    public static Store<String> store;
    
    @Override
    public Arguments getDefaultParameters() {
    
        Arguments args = new Arguments();
        args.addArgument("Config file", "");
        args.addArgument("key", "");
        return args;
    }
    
    public SampleResult runTest(JavaSamplerContext context) {
    
        SampleResult result = new SampleResult();
        result.setSampleLabel("Get");
        result.setDataType(SampleResult.TEXT);
        result.sampleStart();
        try {
            byte b[] = store.get(context.getParameter("key"));
            result.setSuccessful(true);
            result.setResponseCodeOK();
            result.setResponseData(b);
            
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
    
        if (store == null) {
            store = new StoreImpl<String>();
            try {
                store.init(context.getParameter("Config file"));
            } catch (InitializationException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
    
}
