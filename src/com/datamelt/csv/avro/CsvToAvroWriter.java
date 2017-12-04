/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datamelt.csv.avro;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.joda.time.DateTime;

/**
 * Utility class to write CSV rows/records to an Avro file.
 * 
 * Create a CsvToAvroWriter object by supplying the Avro schema and the path
 * and name of the output file to use.
 * 
 * There are two modes available: write and append. So either the data is appended
 * to an existing avro file (append mode) or written to a new file (write mode)
 * 
 * You may set the compression factor for the Avro output file as well as the
 * mode. The data can either be written to a new file or appended to an existing file.
 * 
 * The append method accepts an Avro SpecificRecord and a line/row of data from
 * a CSV file. The line/row will be split into it's single fields using the
 * defined separator or if none is defined the default separator. The resulting
 * Avro record is then appended to the output file.
 * 
 * Use the setCsvHeader() method to define the fields that are present in the CSV file.
 * 
 * If the header row of the CSV is undefined, then it is assumed that the fields
 * in the CSV file are present in the same sequence as they are defined in the
 * Avro schema.
 * 
 * If the header row is defined, then the method will locate the corresponding Avro
 * field with the same name in the schema. In this case the sequence of fields in the
 * CSV file is not relevant - the method will update the correct Avro field.
 * 
 * Don't forget to close the writer using the close() method, after all records
 * are processed.
 * 
 * 
 * 
 * @author uwe - 2017-12-04
 *
 * 
 */
public class CsvToAvroWriter<T extends SpecificRecord>
{
	// constants which define the type of separator used in the csv file
	// to separate the individual columns
	public static final String SEPARATOR_TAB 		= "\t";
	public static final String SEPARATOR_SEMICOLON 	= ";";
	public static final String SEPARATOR_COMMA		= ",";
	public static final String SEPARATOR_AT			= "@";
	public static final String SEPARATOR_AT_AT		= "@@";
	public static final String SEPARATOR_PIPE		= "|";
	
	// we can write to a new file respectively overwrite an existing one
	// or append to an existing one
	public static final int MODE_WRITE				= 0;
	public static final int MODE_APPEND				= 1;
	
	// the default mode is write
	private static int mode 						= MODE_WRITE;
	
	// default compression factor
	public static final int DEFAULT_COMPRESSION		= 6;
	private static Schema NULL_SCHEMA 				= Schema.create(Schema.Type.NULL);
    private DataFileWriter<T> dataFileWriter 		= null;
    private String outputFileName					= null;
    private Schema avroSchema 						= null;
    private String[] csvHeaderFields				= null;
    private Map<String,Integer> fieldMap			= null;
    private String separator						= SEPARATOR_SEMICOLON;
    
    private String csvDateTimeFormat;
    private String csvDateFormat;
    
    /**
	 * Constructor to accept the Avro schema file and the output file name
	 * to write the avro data to. Default compression of 6 will be used.
	 * 
	 * Pass the Avro schema and the path and name of the output file and the
	 * mode (write or append).
	 * An Avro DataFileWriter object will be created for the given output file,
	 * using the default compression.
	 * 
	 * @param schema		schema corresponding to the data
	 * @param outputFile	path and name of the output file
	 * @param mode			either write or append to the avro file
	 * @throws Exception
	 */
	public CsvToAvroWriter(Schema schema, String outputFileName, int mode) throws Exception
	{
		this.avroSchema = schema;
	    this.outputFileName = outputFileName;
	    CsvToAvroWriter.mode = mode;
	    this.getDataFileWriter(DEFAULT_COMPRESSION);
	    
	    // populate the map of avro field names and positions
    	fieldMap = new HashMap<String,Integer>();
    	List<Field> avroFields = avroSchema.getFields();
    	for(int i=0;i<avroFields.size();i++)
    	{
    		Field avroField = avroFields.get(i);
    		fieldMap.put(avroField.name(), avroField.pos());
    	}
	}
	
	/**
	 * Constructor to accept the Avro schema file and the output file name
	 * to write the avro data to.
	 * 
	 * Pass the Avro schema and the path and name of the output file and the
	 * mode (write or append).
	 * An Avro DataFileWriter object will be created for the given output file,
	 * using the default compression.
	 * 
	 * @param schema				schema corresponding to the data
	 * @param outputFile			path and name of the output file
	 * @param mode					either write or append to the avro file
	 * @param compressionFactor		compression factor to use
	 * @throws Exception
	 */
	public CsvToAvroWriter(Schema schema, String outputFileName, int mode, int compressionFactor) throws Exception
	{
		this.avroSchema = schema;
	    this.outputFileName = outputFileName;
	    CsvToAvroWriter.mode = mode;
	    this.getDataFileWriter(compressionFactor);
	}
    
	/**
	 * creates a DatumWriter with the specified Avro schema and then
	 * creates a DataFileWriter object.
	 * 
	 * @param avroSchema	the Avro schema to use
	 * @throws Exception	when the DatumWriter or the DataFileWriter can not be created
	 */
    private void getDataFileWriter(int compressionFactor) throws Exception
    {
		DatumWriter<T> datumWriter = new SpecificDatumWriter<T>(avroSchema);
		DataFileWriter<T> dataFileWriter = new DataFileWriter<T>(datumWriter);
		dataFileWriter.setCodec(CodecFactory.deflateCodec(compressionFactor)); 
		
		if(mode == MODE_WRITE)
		{
			dataFileWriter.create(avroSchema, new File(outputFileName));
		}
		else
		{
			dataFileWriter.appendTo(new File(outputFileName));
		}
		this.dataFileWriter = dataFileWriter;
    }
    
    /**
     * The append method accepts an Avro SpecificRecord, a line/row of data from
     * a CSV file. The line/row will be split into it's single fields using the
     * provided separator.
     * 
     * The resulting Avro record is then appended to the output file.
     * 
     * Use the setCsvHeader() to define the fields that are present in the CSV file.
     * 
     * If the header row of the CSV is undefined, then it is assumed that the fields
     * in the CSV file are present in the same sequence as they are defined in the
     * Avro schema.
     * 
     * If the header row is defined, then this method will locate the corresponding Avro
     * field with the same name in the schema. In this case the sequence of fields in the
     * CSV file is not relevant - the method will update the correct Avro field.
     * 
     * @param record				The SpecificRecord to use
     * @param line					A line/row from a CSV file
     * @param separatorCharacter	The separator used to separate the individual fields
     * @throws Exception
     */
    public void append(T record, String line, String separatorCharacter) throws Exception
    {
    	populate(record, line, separatorCharacter);
    	ArrayList <Field>nullFields = checkFields(record);
    	if(nullFields.size()>0)
    	{
    		throw new Exception("fields with null values but the schema does not allow for null: " + nullFields.toString());
    	}
    	else
    	{
    		dataFileWriter.append(record);
    	}
    }
    
    /**
     * The append method accepts an Avro SpecificRecord, a line/row of data from
     * a CSV file. The line/row will be split into it's single fields using the
     * default separator. Use the setSeparator() method to define a different
     * separator.
     * 
     * The resulting Avro record is then appended to the output file.
     * 
     * Use the setCsvHeader() to define the fields that are present in the CSV file.
     * 
     * If the header row of the CSV is undefined, then it is assumed that the fields
     * in the CSV file are present in the same sequence as they are defined in the
     * Avro schema.
     * 
     * If the header row is defined, then this method will locate the corresponding Avro
     * field with the same name in the schema. In this case the sequence of fields in the
     * CSV file is not relevant - the method will update the correct Avro field.
     * 
     * @param record				The SpecificRecord to use
     * @param line					A line/row from a CSV file
     * @throws Exception
     */
    public void append(T record, String line) throws Exception
    {
    	append(record, line, separator);
    }
    
    /**
     * sets the separator to be used to separate the individual fields/columns of the row
     * of data from a CSV file
     * 
     * @param separator
     */
    public void setSeparator(String separator)
    {
    	this.separator = separator;
    }
    
    public String getSeparator()
	{
		return separator;
	}
    
    /**
     * sets the format of datetime fields as it is used in the CSV file.
     * 
     * if the relevant field is defined as "long" with a logical type "timestamp-millis" this
     * format will be used to parse the csv value and convert it to milliseconds from the unix epoch
     * 
     * @param format
     */
    public void setCsvDateTimeFormat(String format)
    {
    	this.csvDateTimeFormat = format;
    }
    
    
    public String getCsvDateTimeFormat()
	{
		return csvDateTimeFormat;
	}
    
    /**
     * sets the format of date fields as it is used in the CSV file.
     * 
     * if the relevant field is defined as "int" with a logical type "date" this
     * format will be used to parse the csv value and convert it to days from the unix epoch
     * 
     * @param format
     */
    public void setCsvDateFormat(String format)
    {
    	this.csvDateFormat = format;
    }
    
    
    public String getCsvDateFormat()
	{
		return csvDateFormat;
	}

	/**
     * sets the header row taken from the csv file.The given
     * separatorCharacter is used to divide the header into individual fields.
     * 
     * if the header row is defined, the append() method will
     * correctly add the correct values from the CSV file to the 
     * Avro record, even if the CSV files has a different sequence
     * of fields.
     * 
     * @param header
     * @param separatorCharacter
     */
    public void setCsvHeader(String header, String separatorCharacter)
    {
    	this.csvHeaderFields = header.split(separatorCharacter);
    }
    
    public void setCsvHeader(String[] fields)
    {
    	this.csvHeaderFields = fields;
    }
    
    /**
     * sets the header row taken from the csv file. The default
     * separator is used to divide the header into individual fields.
     * 
     * if the header row is defined, the append() method will
     * correctly add the correct values from the CSV file to the 
     * Avro record, even if the CSV files has a different sequence
     * of fields.
     * 
     * @param header
     */
    public void setCsvHeader(String header)
    {
    	setCsvHeader(header, separator);
    }
    
    /**
     * closes the DataFileWriter that is used to produce the Avro output file
     * 
     * @throws Exception
     */
    public void closeWriter() throws Exception
    {
    	dataFileWriter.close(); 
    }
    
    private ArrayList<Field> checkFields(T record) 
    {
    	List<Field> avroFields = avroSchema.getFields();
    	ArrayList<Field>nullFields = new ArrayList<Field>();
    	for (int i=0;i<avroFields.size();i++)
    	{
    		Field field = avroFields.get(i);
    		Object value = record.get(field.pos());
    		
    		if(value==null && !getFieldAllowsNull(field))
    		{
    			nullFields.add(field);
    		}
    	}
    	return nullFields;
    }
    
    /**
     * populates an Avro Specific record with the values from a row of CSV data.
     * 
     * If the header row of the CSV is undefined, then it is assumed that the fields
     * in the CSV file are present in the same sequence as they are defined in the
     * Avro schema,
     * 
     * If the header row is defined, then this method will locate the corresponding Avro
     * field with the same name in the schema. In this case the sequence of fields in the
     * CSV file is not relevant - the method will update the correct Avro field.
     * 
     * @param record				The SpecificRecord to use
     * @param line					a line/row of data from a CSV file
     * @param separatorCharacter	the separator used to separate the individual fields of the CSV row
     * @return						a record with the data of a line from the CSV file
     */
    private T populate(T record,String line, String separatorCharacter) throws Exception
	{
    	String[] fields = line.split(separatorCharacter);
    	
    	// if the names of the fields are defined (header row was specified)
    	if(csvHeaderFields !=null)
    	{
    		// loop of the header fields
    		for(int i=0;i<fields.length;i++)
			{
    			// name of the csv field as defined in the header
    			String csvFieldName = csvHeaderFields[i];
    			
    			// if the field name from the CSV file is present in the Avro schema.
    			// if the equivalent field is not found in the avro schema, then we
    			// ignore it
    			if(fieldMap.containsKey(csvFieldName))
    			{
	    			// get the position of the field with the same name
	    			// in the avro schema
	    			int avroPosition = fieldMap.get(csvFieldName);
	    			
	    			// retrieve the field
	    			Field field = avroSchema.getField(csvFieldName);
	    			
	    			// retrieve a field from the Avro SpecificRecord
					Object object = getObject(field,fields[i]);
					// add the object to the corresponding field
					record.put(avroPosition, object);
    			}
			}
    	}
    	else
    	{
			for(int i=0;i<fields.length;i++)
			{
				List<Field> avroFields = avroSchema.getFields();
				
				Field field = avroFields.get(i);
				// retrieve a field from the Avro SpecificRecord
				Object object = getObject(field,fields[i]);
				// add the object to the corresponding field
				record.put(i, object);
				
			}
    	}
		return record;
	}
    
    /**
     * determines and converts the String value from the CSV file into
     * the correct object (type) according to the Avro schema definition.
     * 
     * @param field		a field from the Avro record 
     * @param value		the value from a CSV
     * @return			the correct object according to the schema
     */
    private Object getObject(Field field, String value) throws Exception
	{
    	// retrieve the field type
    	Type fieldType = getFieldType(field);
    	// retrieve the logical type of the field. relevant for some date and time types
    	LogicalType logicalFieldType = getFieldLogicalType(field);
    	boolean nullAllowed = getFieldAllowsNull(field);
    	
    	if(value!=null && !value.equals(""))
		{
	    	if(fieldType == Schema.Type.INT)
			{
	    		try
				{
	    			if(logicalFieldType!=null && logicalFieldType.getClass() == LogicalTypes.Date.class)
			    	{
						return convertToDays(value);
			    	}
					else
					{
						return Integer.parseInt(value);
					}
				}
				catch (Exception ex)
				{
					throw new Exception("value [" + value + "] could not be converted to an integer");
				}
			}
			else if(fieldType == Schema.Type.LONG)
			{
				try
				{
					if(logicalFieldType!=null && logicalFieldType.getClass() == LogicalTypes.TimestampMillis.class)
			    	{
						return convertToTimestampMilliseconds(value);
			    	}
					else
					{
						return Long.parseLong(value);
					}
				}
				catch (Exception ex)
				{
					throw new Exception("value [" + value + "] could not be converted to a long");
				}
			}
			else if(fieldType == Schema.Type.FLOAT)
			{
				try
				{
					return Float.parseFloat(value);
				}
				catch (Exception ex)
				{
					throw new Exception("value [" + value + "] could not be converted to a float");
				}
			}
			else if(fieldType == Schema.Type.DOUBLE)
			{
				try
				{
					return Double.parseDouble(value);
				}
				catch (Exception ex)
				{
					throw new Exception("value [" + value + "] can not be converted to a double");
				}
			}
			else if(fieldType == Schema.Type.BOOLEAN)
			{
				try
				{
					return Boolean.parseBoolean(value);
				}
				catch (Exception ex)
				{
					throw new Exception("value [" + value + "] can not be converted to a boolean");
				}
			}
			else if(fieldType == Schema.Type.STRING)
			{
				return value;
			}
			else
			{
				throw new Exception("type [" + fieldType + "] not supported for field: [" + field + "]");
			}
		}
		else
		{
			if(value.equals("") && fieldType == Schema.Type.STRING)
			{
				return value;
			}
			else
			{
				if(nullAllowed)
				{
					return null;
				}
				else 
				{
					throw new Exception("field value null is not defined in the schema for field: [" + field + "]");
				}
			}
		}
	}
    
    /**
     * converts the given value to milliseconds from the unix epoch using the
     * defined format.
     * 
     * Note: datetime in an avro schema is defined in number of days
     * 		 from the unix epoche
     * 
     * @param value			the value to convert
     * @return				milliseconds from the unix epoch as a long value
     * @throws Exception	when the value can not be parsed to a long
     */
    private long convertToTimestampMilliseconds(String value) throws Exception
    {
    	SimpleDateFormat sdf = new SimpleDateFormat(csvDateTimeFormat);
    	try
    	{
    		Date date = sdf.parse(value);
    		return new DateTime(date.getTime()).getMillis();
    	}
    	catch(Exception ex)
    	{
    		throw new Exception("the value [" + value + "] cannot be converted to milliseconds from the unix epoch using the specified format [" + csvDateTimeFormat + "]");
    	}
    }
    
    /**
     * converts the given value to days from the unix epoch using the
     * defined format.
     * 
     * Note: dates in an avro schema are defined in number of days
     * 		 from the unix epoche
     * 
     * @param value			the value to convert
     * @return				milliseconds from the unix epoch as a long value
     * @throws Exception	when the value can not be parsed to a long
     */
    private int convertToDays(String value) throws Exception
    {
    	SimpleDateFormat sdf = new SimpleDateFormat(csvDateFormat);
    	try
    	{
    		Date date = sdf.parse(value);
    		
    		long currentMilli = date.getTime();
        	long seconds = currentMilli / 1000;
        	long minutes = seconds / 60;
        	long hours = minutes / 60;
        	long days = hours / 24;
    		return (int)days;
    	}
    	catch(Exception ex)
    	{
    		throw new Exception("the value [" + value + "] cannot be converted to days from the unix epoch using the specified format [" + csvDateFormat + "]");
    	}
    }
    
    
    /**
     * returns the type of the avro schema field
     *  
     * @param field		the avro schema field
     * @return			the type that is defined for the field
     */
    private Type getFieldType(Field field)
    {
    	Type fieldType = field.schema().getType();
    	
    	Type type = null;
    	
    	// if the field is of type union, we must loop to get the correct type
    	// if not then there is only one definition
    	if(fieldType == Schema.Type.UNION)
    	{
    		List<Schema> types = field.schema().getTypes();
    		
    		for(int i=0;i<types.size();i++)
    		{
    			// get the type that does NOT define null as the
    			// possible type
    			if(!types.get(i).equals(NULL_SCHEMA))
        		{
    				type = types.get(i).getType();
    				break;
        		}
    		}
    		return type;
    	}
    	else
    	{
    		return fieldType;
    	}
    }
    
    /**
     * returns the logical field type of an avro schema field
     * 
     * logical type definitions can help to further define which type
     * of field is used in the schema. e.g. a date or time field.
     * 
     * Note: there is a bug in avro 1.8.2. and datetime fields.
     * 		 it is not clear when that will be fixed.
     * 
     * @param field		the avro schema field
     * @return			a logical type definition if one was specified in the avro schema
     */
    private LogicalType getFieldLogicalType(Field field)
    {
    	Type type = field.schema().getType();
    	
    	LogicalType logicalFieldType = null;
    	
    	if(type == Schema.Type.UNION)
    	{
    		List<Schema> types = field.schema().getTypes();
    		
    		if(types.get(0).equals(NULL_SCHEMA))
    		{
    			logicalFieldType = types.get(1).getLogicalType();
    		}
    		else
    		{
    			logicalFieldType = types.get(0).getLogicalType();
    		}
    		return logicalFieldType;
    	}
    	else
    	{
    		return logicalFieldType;
    	}
    }
    
    /**
     * determines if the definition of the field allows null as value
     * 
     * @param field		and avro schema field
     * @return			if the specified field allows null
     */
    private boolean getFieldAllowsNull(Field field)
    {
    	Type type = field.schema().getType();
    	
    	boolean nullAllowed = false;
    	
    	// the null is allowed we have two fields (maybe more): one for
    	// the field type and one defining null
    	if(type == Schema.Type.UNION)
    	{
    		List<Schema> types = field.schema().getTypes();
    		
    		for(int i=0;i<types.size();i++)
    		{
    			if(types.get(i).equals(NULL_SCHEMA))
    			{
    				nullAllowed = true;
    				break;
    			}
    		}
    		return nullAllowed;
    	}
    	else
    	{
    		return nullAllowed;
    	}
    }
}
