# csv2avro

Easy to use Utility class to write CSV rows/records to an Avro file.

Create a CsvToAvroWriter object by supplying the Avro schema and the path
and name of the output file to use.

You may set the compression factor for the Avro output file as well as the
mode. The data can either be written to a new file or appended to an existing file.

The append method accepts an Avro SpecificRecord and a line/row of data from
a CSV file. The line/row will be split into it's single fields using the
defined separator or if none is defined the default separator. The resulting
Avro record is then appended to the output file.

Use the setCsvHeader() to define the fields that are present in the CSV file.

If the header row of the CSV is undefined, then it is assumed that the fields
in the CSV file are present in the same sequence as they are defined in the
Avro schema.

If the header row is defined, then the method will locate the corresponding Avro
field with the same name in the schema. In this case the sequence of fields in the
CSV file is not relevant - the method will update the correct Avro field.

As the first step create the desired Avro schema corresponding to the CSV file. It
may contain all or only part of the CSV file fields. Important is, that the data types
of the fields are correctly defined.

Here is an example:

This is the Avro schema (in this case named: discount.avsc):

	{"namespace": "com.datamelt.avro.discount",
	 "type": "record",
	 "name": "Discount",
	 "fields": [
		{"name": "id", "type": "long"},
		{"name": "age", "type": "int"},
		{"name": "destination_region", "type": "string"},
		{"name": "destination_airport", "type": "string"},
		{"name": "price", "type": "float"},
		{"name": "frequent_traveller", "type": "int"}
		]
	}

Next compile the Avro schema with the Avro tools. Like this:

	java -jar avro-tools-1.8.2.jar compile schema discount.avsc .

Then call the CSVToAvroWriter like this from your code:

CsvToAvroWriter<Discount> writer = new CsvToAvroWriter<Discount>(Discount.SCHEMA$,outputFile,CsvToAvroWriter.MODE_WRITE);

If you have a header row - a row which defines the names of the individual fields in the
CSV file rows, add it like this:

	writer.setCsvHeader(header);

Finally, add your CSV rows containing the data like this:

	writer.append(new Departure(), line);

That's all - an easy way to convert your CSV files. 

Here is a complete code sample:

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.datamelt.csv.avro.CsvToAvroWriter;

	public class DiscountProducer
	{
		public static void main(String[] args) throws Exception
		{
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			
			System.out.println(df.format(new Date()) + " processing records started");

			String folder = "/home/uwe/development/pcm17/avro";
			String outputFile = folder + "/discount_test.avro";

			File inputFile = new File(folder + "/discount_test.csv");
			FileReader fileReader = new FileReader(inputFile);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			
			long counter=0;
			String line;
			
			CsvToAvroWriter<Discount> writer = new CsvToAvroWriter<Discount>(Discount.SCHEMA$,outputFile,CsvToAvroWriter.MODE_WRITE);
			
			while((line = bufferedReader.readLine()) != null)
			{
				counter ++;
				
				writer.append(new Discount(), line);
				
				if(counter % 10000==0)
				{
					System.out.println(df.format(new Date()) + " processed records: " + counter);
				}
			}
			fileReader.close();	
			writer.closeWriter();
			
			System.out.println(df.format(new Date()));
			System.out.println(df.format(new Date()) + " processing records complete");
			System.out.println(df.format(new Date()) + " total processed records: " + counter);
		}
	}

Please send your feedback.

last update: Uwe Geercken - 2017-11-18
