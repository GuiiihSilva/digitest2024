package Task;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class RestApiClient {

    private static final String API_URL = "https://3ospphrepc.execute-api.us-west-2.amazonaws.com/prod/RDSLambda";
    private static final String BUCKET_NAME = "arqjson4";
    private static final String FILE_NAME = "Gênero_Contagem_Final.txt";

    public static void main(String[] args) {
        try {
            // Passo 1: Fazer a solicitação para a API REST
            String jsonResponse = fetchApiData();
            System.out.println("Resposta da API: " + jsonResponse);

            // Passo 2: Processar os dados para contar os registros por gênero
            Map<String, Integer> genderCounts = countGenders(jsonResponse);
            System.out.println("Contagem dos gêneros: " + genderCounts);

            // Passo 3: Salvar o resultado em um arquivo
            File file = saveToFile(genderCounts);
            System.out.println("Dados salvos no arquivo: " + file.getAbsolutePath());

            // Passo 4: Criar o bucket S3 (se não existir) e enviar o arquivo para o bucket S3 da AWS
            createBucketIfNotExists();
            uploadToS3();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String fetchApiData() throws IOException {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(RestApiClient.API_URL);
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                return EntityUtils.toString(response.getEntity());
            }
        }
    }

    private static Map<String, Integer> countGenders(String jsonResponse) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(jsonResponse);
        Map<String, Integer> genderCounts = new HashMap<>();

        Iterator<JsonNode> elements = rootNode.elements();
        while (elements.hasNext()) {
            JsonNode element = elements.next();
            JsonNode genderNode = element.get("gender");
            if (genderNode != null) {
                String gender = genderNode.asText();
                genderCounts.put(gender, genderCounts.getOrDefault(gender, 0) + 1);
            }
        }

        return genderCounts;
    }

    private static File saveToFile(Map<String, Integer> genderCounts) throws IOException {
        File file = new File(FILE_NAME);
        try (FileWriter writer = new FileWriter(file)) {
            for (Map.Entry<String, Integer> entry : genderCounts.entrySet()) {
                writer.write(entry.getKey() + ": " + entry.getValue() + "\n");
            }
        }

        return file;
    }

    private static void createBucketIfNotExists() {
        Region region = Region.US_EAST_2;
        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();

        try (S3Client s3 = S3Client.builder().region(region)
                .credentialsProvider(credentialsProvider)
                .build()) {

            // Verifica se o bucket já existe
            try {
                s3.headBucket(HeadBucketRequest.builder().bucket(BUCKET_NAME).build());
                System.out.println("Bucket já existe: " + BUCKET_NAME);
            } catch (NoSuchBucketException e) {
                // Se o bucket não existir, cria-o
                CreateBucketRequest createBucketRequest = CreateBucketRequest.builder()
                        .bucket(BUCKET_NAME)
                        .createBucketConfiguration(CreateBucketConfiguration.builder()
                                .locationConstraint(region.id())
                                .build())
                        .build();
                s3.createBucket(createBucketRequest);
                System.out.println("Bucket criado: " + BUCKET_NAME);
            }

        } catch (S3Exception e) {
            System.err.println("Erro ao verificar ou criar o bucket: " + e.getMessage());
        }
    }

    private static void uploadToS3() {
        Region region = Region.US_EAST_2;
        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();

        try (S3Client s3 = S3Client.builder().region(region)
                .credentialsProvider(credentialsProvider)
                .build()) {

            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(FILE_NAME)
                    .build();

            s3.putObject(putOb, Paths.get(FILE_NAME));
            System.out.println("Arquivo enviado para o S3 com sucesso.");
        } catch (S3Exception e) {
            System.err.println("Erro ao enviar arquivo para o S3: " + e.getMessage());
        }
    }
}