package com.hue.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.lang3.StringUtils;

public final class SecurityUtils {
	private static final String S = "a7eb46cb-79df-4b62-a771-f9d6c33bbc94";
	private static final String R = "AjoYulinTaiRobGuillermo";
	private static final String PASSWORD_PREFIX = "{vero-#}";
	private static final String ENCRYPTION_ALG = "AES/ECB/PKCS5Padding";
	private static final int MAX_ITERATIONS = 10;

	private SecurityUtils() {
	}

	public static byte[] getSHA256HashValue(String input) throws NoSuchAlgorithmException {
		MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
		return sha256.digest(input.getBytes(StandardCharsets.UTF_8));
	}
	
 	public static String base64Encoding(byte[] bArray) {
 		return Base64.getUrlEncoder().encodeToString(bArray);
 	}
 	
 	public static byte[] base64Decoding(String input) {
 		return Base64.getUrlDecoder().decode(input);
 	}
 	
 	public static String encryptMessage(String msg, SecretKey secretKey) throws Exception {
 		Cipher cipher = Cipher.getInstance(ENCRYPTION_ALG);
 		cipher.init(Cipher.ENCRYPT_MODE, secretKey);
 		byte[] encrypted = cipher.doFinal(msg.getBytes(StandardCharsets.UTF_8));
 		return base64Encoding(encrypted);
 	}
 	
 	public static String decryptMessage(String msg, SecretKey secretKey) throws Exception {
 		Cipher cipher = Cipher.getInstance(ENCRYPTION_ALG);
 		cipher.init(Cipher.DECRYPT_MODE, secretKey);
 		byte[] decrypted = cipher.doFinal(base64Decoding(msg));
 		return new String(decrypted, StandardCharsets.UTF_8);
 	}
 	
 	public static SecretKey getAESKey(String input) throws NoSuchAlgorithmException {
 		byte[] hashCode = getSHA256HashValue(input);
 		SecretKey key = new SecretKeySpec(Arrays.copyOf(hashCode, 16), "AES");
 		
 		return key;
 	}
 	
 	public static String createAuthHeader(String authToken, String email) throws NoSuchAlgorithmException, Exception {
 		return "Token token=" + SecurityUtils.encryptMessage(authToken, getAESKey(StringUtils.reverse(email))) + ", email=" + email;
 	}
 	
 	public static String encryptPassword(String password) throws NoSuchAlgorithmException, Exception {
 		if (password == null || password.trim().equals("") || password.startsWith(PASSWORD_PREFIX)) return password;
 		
 		return PASSWORD_PREFIX + encryptMessage(password, getAESKey(S + StringUtils.reverse(R)));
 	}
 	
 	public static String decryptPassword(String encryptedPassword) throws NoSuchAlgorithmException, Exception {
 		if (encryptedPassword == null || encryptedPassword.trim().equals("") || !encryptedPassword.startsWith(PASSWORD_PREFIX)) return encryptedPassword;
 		
 		return decryptMessage(encryptedPassword.substring(PASSWORD_PREFIX.length()), getAESKey(S + StringUtils.reverse(R)));
 	}

 	public static String createPricingUpgradeParams(String email, String token) throws Exception {
 		return "email=" + email + "&token=" 
				+ SecurityUtils.encryptMessage(token, SecurityUtils.getAESKey(StringUtils.reverse(email)));	
 	}
 	
	public static int getRandomUnusedLocalPort() {
        int rndPort;
        int iterations = 0;

        do {
            rndPort = (int) (Math.random() * 55535d + 10000d);
            iterations++;

            if (iterations >= MAX_ITERATIONS) {
                return -1;
            }
        } while (!available(rndPort));

        return rndPort;
    }

	private static boolean available(int port) {
        try (ServerSocket ignored = new ServerSocket(port, 1, InetAddress.getLoopbackAddress())) {
            return true;
        } catch (IOException ignored) {
            return false;
        }
    }
}
