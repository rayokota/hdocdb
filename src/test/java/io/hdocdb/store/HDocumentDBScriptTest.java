package io.hdocdb.store;

import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Value;
import org.junit.BeforeClass;
import org.junit.Test;
import org.ojai.Document;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Objects;
import java.util.function.Predicate;

public class HDocumentDBScriptTest {

    private static boolean useMock = HDocumentDBTest.useMock;
    private static ScriptEngineManager manager = new ScriptEngineManager();
    private static ScriptEngine engine;
    static {
        HostAccess customMapping = HostAccess.newBuilder()
            // The following options are from HostAccess.ALL, and we add the
            // targetTypeMapping to disambugate calls to overridden methods
            // that take both Document and Value
            .allowPublicAccess(true)
            .allowAllImplementations(true)
            .allowAllClassImplementations(true)
            .allowArrayAccess(true)
            .allowListAccess(true)
            .allowBufferAccess(true)
            .allowIterableAccess(true)
            .allowIteratorAccess(true)
            .allowMapAccess(true)
            .targetTypeMapping(org.graalvm.polyglot.Value.class, Document.class,
                s -> s.isHostObject() && s.asHostObject() instanceof Document,
                Value::asHostObject,
                HostAccess.TargetMappingPrecedence.HIGHEST)
            .build();
        engine = GraalJSScriptEngine.create(null,
            Context.newBuilder("js")
                .allowHostAccess(customMapping)
                .allowHostClassLookup(s -> true)
                .option("js.ecmascript-version", "2021")
                // The following two options are for Nashorn syntax extensions
                .allowExperimentalOptions(true)
                .option("js.syntax-extensions", "true"));
    }

    @BeforeClass
    public static void init() throws Exception {
        engine.put("useMock", useMock);
        evalScript("/util/assert.js");
        evalScript("/util/setup.js");
    }

    @Test
    public void testBasic() throws Exception {
        evalScript("/jstests/basic.js");
    }

    @Test
    public void testBasic1() throws Exception {
        evalScript("/jstests/basic1.js");
    }

    @Test
    public void testBasic2() throws Exception {
        evalScript("/jstests/basic2.js");
    }

    @Test
    public void testBasic3() throws Exception {
        evalScript("/jstests/basic3.js");
    }

    @Test
    public void testBasic4() throws Exception {
        evalScript("/jstests/basic4.js");
    }

    @Test
    public void testBasic5() throws Exception {
        evalScript("/jstests/basic5.js");
    }

    @Test
    public void testBasic7() throws Exception {
        evalScript("/jstests/basic7.js");
    }

    @Test
    public void testBasic8() throws Exception {
        evalScript("/jstests/basic8.js");
    }

    @Test
    public void testBasic9() throws Exception {
        evalScript("/jstests/basic9.js");
    }

    @Test
    public void testFind1() throws Exception {
        evalScript("/jstests/find1.js");
    }

    @Test
    public void testFind2() throws Exception {
        evalScript("/jstests/find2.js");
    }

    @Test
    public void testFind4() throws Exception {
        evalScript("/jstests/find4.js");
    }

    @Test
    public void testFind5() throws Exception {
        evalScript("/jstests/find5.js");
    }

    @Test
    public void testFind6() throws Exception {
        evalScript("/jstests/find6.js");
    }

    @Test
    public void testFind7() throws Exception {
        evalScript("/jstests/find7.js");
    }

    @Test
    public void testUpdate2() throws Exception {
        evalScript("/jstests/update2.js");
    }

    @Test
    public void testUpdate3() throws Exception {
        evalScript("/jstests/update3.js");
    }

    @Test
    public void testUpdate5() throws Exception {
        evalScript("/jstests/update5.js");
    }

    @Test
    public void testUpdate6() throws Exception {
        evalScript("/jstests/update6.js");
    }

    @Test
    public void testUpdate7() throws Exception {
        evalScript("/jstests/update7.js");
    }

    @Test
    public void testUpdate8() throws Exception {
        evalScript("/jstests/update8.js");
    }

    @Test
    public void testUpdate9() throws Exception {
        evalScript("/jstests/update9.js");
    }

    @Test
    public void testUpdatea() throws Exception {
        evalScript("/jstests/updatea.js");
    }

    @Test
    public void testUpdatec() throws Exception {
        evalScript("/jstests/updatec.js");
    }

    @Test
    public void testUpdated() throws Exception {
        evalScript("/jstests/updated.js");
    }

    @Test
    public void testUpdatee() throws Exception {
        evalScript("/jstests/updatee.js");
    }

    @Test
    public void testUpdateArrayMatch1() throws Exception {
        evalScript("/jstests/update_arraymatch1.js");
    }

    @Test
    public void testUpdateArrayMatch2() throws Exception {
        evalScript("/jstests/update_arraymatch2.js");
    }

    @Test
    public void testUpdateArrayMatch3() throws Exception {
        evalScript("/jstests/update_arraymatch3.js");
    }

    @Test
    public void testUpdateArrayMatch4() throws Exception {
        evalScript("/jstests/update_arraymatch4.js");
    }

    private static void evalScript(String script) throws Exception {
        String scriptFile = HDocumentDBScriptTest.class.getResource(script).getFile();
        try {
            FileReader reader = new FileReader(scriptFile);
            engine.eval(reader);
        } catch (IOException | ScriptException e){
            e.printStackTrace();
            throw e;
        }
    }
}
