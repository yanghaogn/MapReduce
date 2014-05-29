import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


public class dataTransfer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			FileReader reader=new FileReader(new File("/home/yang/桌面/AdativeScheduler.java"));
			BufferedReader in=new BufferedReader(reader);
			File outFile=new File("/home/yang/桌面/1.8G.java");
			FileWriter out=new FileWriter(outFile);
			if(!outFile.exists())
				outFile.createNewFile();
			int n=0;
			while(true){
				String line=in.readLine();
				if(null==line) break;
				if(line.length()>1)
					line=line.substring(1);
				else line="";
				line+="\r\n";
				n++;
				out.write(line);
  				
			}
			in.close();
			reader.close();
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Success...");

	}

}
