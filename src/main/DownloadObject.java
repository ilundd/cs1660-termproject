package main;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;

public class DownloadObject {
	
	public void downloadObject(String bucketName, String objectName, Path destFilePath) throws FileNotFoundException, IOException {
		
		StorageOptions storageOptions = StorageOptions.newBuilder()
				.setProjectId("termproject-273618")
				.setCredentials(GoogleCredentials.fromStream(new
						FileInputStream("src/main/termproject-273618-3c54da7f1d90.json"))).build();
		Storage storage = storageOptions.getService();
		
		Blob blob = storage.get(BlobId.of(bucketName, objectName));
		blob.downloadTo(destFilePath);
		System.out.println("Downloaded object "
								+ objectName
								+ "\n from bucket name \n"
								+ bucketName
								+ "\n to \n"
								+ destFilePath
								+ "\n");
	}
}
