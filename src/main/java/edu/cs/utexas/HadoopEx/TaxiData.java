package edu.cs.utexas.HadoopEx;

import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

// Class that stores data for each line
public class TaxiData {
	private String hackLic;
	private String medallion;
	private int duration;
	private float totalAmt;
	private boolean valid; // initial value false

	// Generic constructor
	public TaxiData() {
		this.hackLic = "";
		this.medallion = "";
		this.totalAmt = 0.0f;
		this.duration = 0;
		this.valid = true;
	}

	// Deserialize constructor
	public TaxiData(String hackLic, String medallion, float totalAmt, int duration) {
		this.hackLic = hackLic;
		this.medallion = medallion;
		this.totalAmt = totalAmt;
		this.duration = duration;
		this.valid = true;
	}

	// TaxiData custom constructor
	public TaxiData(String line) {
		// this.line = line;
		String[] data = line.split(",");

		float fare_amt = -0.0f;
		float surcharge = -0.0f;
		float mta_tax = -0.0f;
		float tip = -0.0f;
		float tolls = -0.0f;

		// Run error checking
		if (data.length == 17) {
			try {
				fare_amt = Float.parseFloat(data[11]);
				surcharge = Float.parseFloat(data[12]);
				mta_tax = Float.parseFloat(data[13]);
				tip = Float.parseFloat(data[14]);
				tolls = Float.parseFloat(data[15]);

				float calculatedAmt = (float) (Math.round((fare_amt + surcharge + mta_tax + tip + tolls) * 100.0)
						/ 100.0);
				float parsedTotalAmt = Float.parseFloat(data[16]);
				if ((parsedTotalAmt <= 500) && (parsedTotalAmt == calculatedAmt)) {
					// Set information
					this.hackLic = data[1];
					this.medallion = data[0];
					//System.out.println(data[4]);
					this.duration = Integer.parseInt(data[4]);
					this.totalAmt = parsedTotalAmt;
					this.valid = true;
					if (this.duration <= 0) {
						this.valid = false;
					}
				} else {
					System.out.println("Error line: " + line);
				}
				
				// if (!this.valid) {
				// 	System.out.println("Error line: " + line);
				// }
			} catch (Exception e) {
				System.out.println("Error line: " + line);
			}
		} else {
			System.out.println("Error line: " + line);
		}

	}

	public int hash() {
		return medallion != null ? medallion.hashCode() : 0;
	}

	public float getTotalAmount() {
		return this.totalAmt;
	}

	public void setTotalAmount(float totalAmt) {
		this.totalAmt = totalAmt;
	}

	public String getHackLic() {
		return this.hackLic;
	}

	public void setHackLic(String hackLic) {
		this.hackLic = hackLic;
	}

	public int getDuration() {
		return this.duration;
	}

	public void setDuration(int duration) {
		this.duration = duration;
	}

	public String getMedallion() {
		return this.medallion;
	}

	public void setMedallion(String medallion) {
		this.medallion = medallion;
	}

	public boolean isValid() {
		return this.valid;
	}

	// Used for returning value of each TaxiData item
	public String toString() {
		return String.valueOf("Hack: " + hackLic + " Medallion: " + medallion + " Duration: " + duration + " Total:" + totalAmt);
	}

	// Used for comparing TaxiData items to each other
	// public int compareTo(TaxiData o) {
	// 	if (this.totalAmt > o.totalAmt) {
	// 		return 1;
	// 	} else if (this.totalAmt == o.totalAmt) {
	// 		return 0;
	// 	} else {
	// 		return -1;
	// 	}
	// }

	/**
	 * This method manually serializes a data object of this type into a byte array.
	 * Order of writing the data into byte array matters and should be de-serialized
	 * in the same order.
	 * 
	 * @return
	 */
	public byte[] handSerializationWithByteBuffer() {
		// Encodes the hacLic into the byte buffer

		byte[] licBytes = hackLic.getBytes(Charset.forName("UTF-8"));
		byte[] medallionBytes = medallion.getBytes(Charset.forName("UTF-8"));
		// 8 bytes for each float number (two float numbers 16)
		// 4 byte for an integer to write the length of the string
		// lineBytes.length for the legth of the string.

		ByteBuffer byteBuffer = ByteBuffer.allocate(4 + licBytes.length + 4 + 4 + medallionBytes.length + 8);

		// 1. First length of it and then its bytes
		// Each string value might be of different size. We have to write down its
		// length.
		byteBuffer.putInt(licBytes.length);
		byteBuffer.put(licBytes);

		byteBuffer.putInt(duration);

		// 2. String line - of the medaillion license
		byteBuffer.putInt(medallionBytes.length);
		byteBuffer.put(medallionBytes);

		byteBuffer.putFloat(totalAmt);

		return byteBuffer.array();
	}

	/**
	 * This method manually de-serializes an object of DataItem from a given byte
	 * array.
	 * 
	 * @param buf
	 * @return
	 */
	public TaxiData deserializeFromBytes(byte[] buf) {

		ByteBuffer byteBuffer = ByteBuffer.wrap(buf);

		// 1. Read the line string back from the byte array.

		int licSize = byteBuffer.getInt();
		String hackLic = extractString(byteBuffer, licSize);

		int duration = byteBuffer.getInt();

		int medallionSize = byteBuffer.getInt(); // 4 bytes
		String medallion = extractString(byteBuffer, medallionSize);

		// 2. read a float from the given byte array
		float totalAmt = byteBuffer.getFloat();

		return new TaxiData(hackLic, medallion, totalAmt, duration);
	}

	/**
	 * This method reads a string from a buteBuffer.
	 * 
	 * @param byteBuffer
	 * @param stringSize
	 * @return
	 */
	String extractString(ByteBuffer byteBuffer, int stringSize) {
		byte[] stringBytes = new byte[stringSize];
		byteBuffer.get(stringBytes, 0, stringSize);

		String mystring = new String(stringBytes, Charset.forName("UTF-8"));
		return mystring;
	}
}