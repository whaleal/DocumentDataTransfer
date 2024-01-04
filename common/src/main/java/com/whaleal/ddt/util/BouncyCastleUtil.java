//package com.whaleal.ddt.util;
//
//import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
//import org.bouncycastle.cert.X509CertificateHolder;
//import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
//import org.bouncycastle.openssl.PEMParser;
//import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
//import org.bouncycastle.util.io.pem.PemObject;
//import org.bouncycastle.util.io.pem.PemWriter;
//import org.springframework.util.ObjectUtils;
//
//import javax.net.ssl.KeyManagerFactory;
//import javax.net.ssl.SSLContext;
//import javax.net.ssl.TrustManagerFactory;
//import java.io.*;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.security.*;
//import java.security.cert.CertificateFactory;
//import java.security.cert.X509Certificate;
//import java.security.spec.InvalidKeySpecException;
//import java.security.spec.PKCS8EncodedKeySpec;
//import java.util.Base64;
//
///**
// * @author lyz
// * @desc
// * @create: 2023-12-15 14:27
// **/
//public class BouncyCastleUtil {
//
//    /**
//     * 证书字符串 -> 证书
//     * @param certificatePEM
//     * @return
//     * @throws Exception
//     */
//    public static X509Certificate getCertificateFromCertificate(String certificatePEM) throws Exception {
//
//        //从证书中获取publicKey
//        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
//        String certificatePEMContent = certificatePEM.replace("-----BEGIN CERTIFICATE-----", "")
//                .replace("-----END CERTIFICATE-----", "")
//                .replaceAll("\\s", "");
//
//        byte[] certificateBytes = Base64.getDecoder().decode(certificatePEMContent);
//
//        try (InputStream inputStream = new ByteArrayInputStream(certificateBytes)) {
//            return  (X509Certificate) certificateFactory.generateCertificate(inputStream);
//        }
//    }
//
//    /**
//     * 密钥字符串 -> 密钥
//     * @param privateKeyPEM
//     * @return
//     * @throws NoSuchAlgorithmException
//     * @throws InvalidKeySpecException
//     */
//    public static PrivateKey getPrivateKey(String privateKeyPEM) throws NoSuchAlgorithmException, InvalidKeySpecException {
//        //获取私钥
//        String privateKeyPEMContent = privateKeyPEM.replace("-----BEGIN PRIVATE KEY-----", "")
//                .replace("-----END PRIVATE KEY-----", "")
//                .replaceAll("\\s", ""); // Remove whitespace characters
//
//        byte[] privateKeyBytes = Base64.getDecoder().decode(privateKeyPEMContent);
//
//        KeyFactory keyFactory = KeyFactory.getInstance("RSA"); // Assuming RSA algorithm, adjust if needed
//        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(privateKeyBytes);
//
//        return keyFactory.generatePrivate(keySpec);
//    }
//
//    /**
//     * 证书/公钥 转为字符串
//     *
//     * @param privateKey
//     * @param certificate
//     * @param  type 文件类型  1: privateKey
//     *             2: CERTIFICATE
//     * @return
//     * @throws Exception
//     */
//    public static String saveCombinedPEM(PrivateKey privateKey, X509Certificate certificate,int type) throws Exception {
//        StringWriter sw = new StringWriter();
//        try (PemWriter pemWriter = new PemWriter(sw)) {
//            if(type == 1){
//                //key
//                pemWriter.writeObject(new PemObject("PRIVATE KEY",privateKey.getEncoded()));
//            }else if(type == 2){
//                //cer
//                pemWriter.writeObject(new PemObject("CERTIFICATE", certificate.getEncoded()));
//            }
//        }
//        return sw.toString();
//    }
//
//
//    /**
//     *
//     * @param certificate
//     * @param fos
//     * @throws Exception
//     */
//    public static void downloadASFile(Certificate certificate, OutputStream fos) throws Exception {
//        PrivateKey privateKey1 = null;
//        if(!ObjectUtils.isEmpty(certificate.getPrivateKey())){
//            privateKey1 = getPrivateKey(certificate.getPrivateKey());
//        }
//
//        X509Certificate certificateFromCertificate = getCertificateFromCertificate(certificate.getCertificate());
//        downloadASFile(privateKey1,certificateFromCertificate,fos);
//    }
//
//        /**
//         * 下载证书文件
//         *
//         * @param privateKey
//         * @param certificate
//         * @return
//         * @throws Exception
//         */
//    public static void downloadASFile(PrivateKey privateKey, X509Certificate certificate, OutputStream fos) throws Exception {
//        try (OutputStreamWriter osw = new OutputStreamWriter(fos)) {
//            PemWriter pemWriter = new PemWriter(osw);
//            if(!ObjectUtils.isEmpty(privateKey)){
//                pemWriter.writeObject(new PemObject("PRIVATE KEY", privateKey.getEncoded()));
//            }
//            if(!ObjectUtils.isEmpty(certificate)){
//                pemWriter.writeObject(new PemObject("CERTIFICATE", certificate.getEncoded()));
//            }
//            pemWriter.close();
//        }
//    }
//
//    /**
//     * 基于capath 和 clientPath 创建 SSLContext
//     * mongo原生驱动基于这个 创建 mongoClient
//     * @param caPath
//     * @param clientPath
//     * @return
//     * @throws Exception
//     */
//    public static SSLContext initContext(String caPath,String clientPath) throws Exception{
//        TrustManagerFactory tmf;
//        try (InputStream is = Files.newInputStream(Paths.get(caPath))) {
//
//            CertificateFactory cf = CertificateFactory.getInstance("X.509");
//            X509Certificate caCert = (X509Certificate) cf.generateCertificate(is);
//            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
//            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
//            ks.load(null);
//            ks.setCertificateEntry("caCert", caCert);
//            tmf.init(ks);
//        }
//
//        // client key
//        KeyManagerFactory keyFac;
//        try (FileReader fr = new FileReader(clientPath)) {
//            KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
//
//            keystore.load(null);
//            PEMParser pemParser = new PEMParser(fr);
//
//            PrivateKeyInfo privateKeyInfo = (PrivateKeyInfo) pemParser.readObject();
//            JcaPEMKeyConverter converter = new JcaPEMKeyConverter();
//            PrivateKey privateKey = converter.getPrivateKey(privateKeyInfo);
//            JcaX509CertificateConverter certConverter = new JcaX509CertificateConverter();
//            X509CertificateHolder certificateHolder = (X509CertificateHolder) pemParser.readObject();
//            X509Certificate certificate = certConverter.getCertificate(certificateHolder);
//
//            //这里第三个参数是 密码 和 keyFac.init时的要保持一致 ，这里设为空
//            keystore.setKeyEntry("alias", privateKey, "".toCharArray(), new X509Certificate[]{certificate});
//            keyFac = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
//            keyFac.init(keystore, "".toCharArray());
//        }
//
//        SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
//        sslContext.init(keyFac.getKeyManagers(), tmf.getTrustManagers(), null);
//
//        return sslContext;
//    }
//
//}
