package main;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class UploadObject {

	public void uploadObject(String bucketName, String objectName, String filePath) throws IOException {
		
		StorageOptions storageOptions = StorageOptions.newBuilder()
				.setProjectId("termproject-273618")
				.setCredentials(GoogleCredentials.fromStream(new
						FileInputStream("src/main/termproject-273618-3c54da7f1d90.json"))).build();
		Storage storage = storageOptions.getService();
		BlobId blobId = BlobId.of(bucketName, objectName);
		BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
		storage.create(blobInfo, Files.readAllBytes(Paths.get(filePath)));
		
		System.out.println("File " 
								+ filePath 
								+ " uploaded to bucket " 
								+ bucketName + " as " 
								+ objectName);
	}
	
}
