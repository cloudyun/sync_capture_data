/**
 * 
 */
package com.wave.util;

public class DataUtil {

	public static byte[] stringArr2ByteArr(String[] array) {
    	byte[] bytes = new byte[array.length * 4];
        for (int i = 0; i < 256; i++) {
        	byte[] tmp = float2byte(Float.parseFloat(array[i]));
        	bytes[i*4+0] = tmp[0];
        	bytes[i*4+1] = tmp[1];
        	bytes[i*4+2] = tmp[2];
        	bytes[i*4+3] = tmp[3];
        }
        return bytes;
    }

	public static byte[] doubleArr2ByteArr(Double[] array) {
    	byte[] bytes = new byte[array.length * 4];
        for (int i = 0; i < array.length; i++) {
        	double d = array[i];
        	byte[] tmp = float2byte(((float) d));
        	bytes[i*4+0] = tmp[0];
        	bytes[i*4+1] = tmp[1];
        	bytes[i*4+2] = tmp[2];
        	bytes[i*4+3] = tmp[3];
        }
        return bytes;
    }

	public static byte[] floatArr2ByteArr(float[] array) {
    	byte[] bytes = new byte[array.length * 4];
        for (int i = 0; i < 256; i++) {
        	byte[] tmp = float2byte(array[i]);
        	bytes[i*4+0] = tmp[0];
        	bytes[i*4+1] = tmp[1];
        	bytes[i*4+2] = tmp[2];
        	bytes[i*4+3] = tmp[3];
        }
        return bytes;
    }

	public static Float[] convertBase64ToFloatArray(String feature) {
		byte[] buf = Base64.decode(feature);
		int startIndex = 0;
		int length = buf.length / 4;
		Float[] featureFloat = new Float[length];
		for (int i = 0; i < length; i++) {
			featureFloat[i] = byte2float(buf, startIndex);
			startIndex += 4;
		}
		return featureFloat;
	}

	public static float byte2float(byte[] b, int index) {

		int l;
		l = b[index + 0];
		l &= 0xff;
		l |= ((long) b[index + 1] << 8);
		l &= 0xffff;
		l |= ((long) b[index + 2] << 16);
		l &= 0xffffff;
		l |= ((long) b[index + 3] << 24);
		return Float.intBitsToFloat(l);
	}
	
	public static byte[] float2byte(float f) {  
        // 把float转换为byte[]  
        int fbit = Float.floatToIntBits(f);  
          
        byte[] b = new byte[4];    
        for (int i = 0; i < 4; i++) {    
            b[i] = (byte) (fbit >> (24 - i * 8));    
        }   
          
        // 翻转数组  
        int len = b.length;  
        // 建立一个与源数组元素类型相同的数组  
        byte[] dest = new byte[len];  
        // 为了防止修改源数组，将源数组拷贝一份副本  
        System.arraycopy(b, 0, dest, 0, len);  
        byte temp;  
        // 将顺位第i个与倒数第i个交换  
        for (int i = 0; i < len / 2; ++i) {  
            temp = dest[i];  
            dest[i] = dest[len - i - 1];  
            dest[len - i - 1] = temp;  
        }  
          
        return dest;  
    }
}
