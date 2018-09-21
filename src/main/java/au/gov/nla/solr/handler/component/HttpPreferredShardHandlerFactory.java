package au.gov.nla.solr.handler.component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Use a preferred node for searching.
 * <p>
 * Read a list(from a file) of preferred nodes to use when searching. If the preferred node
 * is down that use any node that is available.
 * 
 * @author icaldwell
 *
 */
public class HttpPreferredShardHandlerFactory extends HttpShardHandlerFactory{
	private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private List<String> preferred = new ArrayList<>();
	
	@Override
	public void init(PluginInfo info){
		super.init(info);
		// get file name to 
		NamedList<?> args = info.initArgs;
		String fileName = getParameter(args, "fileNamePreferredNodes", "", null);
		if(fileName.isEmpty()){
			log.warn("fileNamePreferredNodes File not provided.");
			return;    	
		}
		File f = new File(fileName);
		if(!f.exists()){
			log.warn("fileNamePreferredNodes File not found. {}",fileName);
			return;
		}
		if(!f.canRead()){
			log.warn("fileNamePreferredNodes File not readable. {}",fileName);
			return;
		}
		BufferedReader reader = null;
		try{
			reader = new BufferedReader(new FileReader(f));
			String line;
			while((line = reader.readLine())!= null){
				if(!line.isEmpty()){
					line = line.trim();
					if(line.startsWith("#")){
						// ignore comment
					}
					else{
						preferred.add(line);
					}
				}
			}
			log.info("fileNamePreferredNodes File loaded {} lines.", preferred.size());
		}catch (IOException e){
			log.error("Error reading fileNamePreferredNodes." + fileName, e);
		}
		finally {
			if(reader != null){
				try{
					reader.close();
				}catch (IOException e){
					// ignore
					log.warn("Ignored Error closing fileNamePreferredNodes." + fileName, e);
				}
			}
		}
	}
	
	/**
	 * Creates a preferred list of urls for the given shard.
	 */
	@Override
	public List<String> makeURLList(String shard){
		List<String> list = super.makeURLList(shard);
		// reorder list so that a preferred node is used over another node
		List<String> newList = new ArrayList<String>();
		for(String s : list){
			if(preferred.contains(s)){
				newList.add(0, s);
			}
			else{
				if(newList.isEmpty()){
					newList.add(0, s);
				}
				else{
					newList.add(1, s);
				}
			}
		}
		return newList;
	}
}
