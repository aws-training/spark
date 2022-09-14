import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import java.io.*;
import org.apache.pig.FilterFunc;
public class IsGoodQuality extends FilterFunc {
	@Override
	public Boolean exec(Tuple tuple) throws IOException {
		if (tuple == null || tuple.size() == 0) {
			return false;
		}
	try{
		Object object = tuple.get(0);
		if (object == null) {
		return false;
	}
	int i = (Integer) object;
	return (i == 1 || i == 2 );
		

	}catch(IOException e){
		throw new IOException(e);
	}

}
	
}
