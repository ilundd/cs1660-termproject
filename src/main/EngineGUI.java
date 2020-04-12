package main;

import java.awt.EventQueue;
import java.awt.Font;
import java.awt.SystemColor;
import java.awt.Window.Type;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.logging.Logger;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.SwingConstants;

import org.apache.commons.io.FileUtils;
import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.InstanceList;
import com.google.api.services.compute.model.NetworkInterface;
import com.google.api.services.dataproc.*;
import com.google.api.services.dataproc.model.Cluster;
import com.google.api.services.dataproc.model.ClusterConfig;
import com.google.api.services.dataproc.model.HadoopJob;
import com.google.api.services.dataproc.model.InstanceGroupConfig;
import com.google.api.services.dataproc.model.Job;
import com.google.api.services.dataproc.model.JobPlacement;
import com.google.api.services.dataproc.model.JobReference;
import com.google.api.services.dataproc.model.JobStatus;
import com.google.api.services.dataproc.model.SubmitJobRequest;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableList;
import com.google.gson.InstanceCreator;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.google.api.services.plus.Plus;
import com.google.api.services.plus.PlusScopes;


public class EngineGUI {

	private JFrame frmIansSearchEngine;
	private JLabel lblTitle;
	private JButton btnChooseFiles;
	private JTextArea txtFileNames;
	private JScrollPane scrollPane;
	private JButton btnLoadEngine;
	
	private FileChooser chooser;
	
	private Logger logger;
	
	private final String bucketId = "dataproc-staging-us-west1-233708547529-zvfgtqye";
	

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					EngineGUI window = new EngineGUI();
					window.frmIansSearchEngine.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the application.
	 */
	public EngineGUI() {
		initialize();
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		frmIansSearchEngine = new JFrame();
		frmIansSearchEngine.setTitle("Ian's Search Engine");
		frmIansSearchEngine.setType(Type.UTILITY);
		frmIansSearchEngine.setResizable(false);
		frmIansSearchEngine.setBounds(100, 100, 735, 555);
		frmIansSearchEngine.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frmIansSearchEngine.getContentPane().setLayout(null);
		
		lblTitle = new JLabel("Load My Engine");
		lblTitle.setFont(new Font("Tahoma", Font.BOLD, 24));
		lblTitle.setHorizontalAlignment(SwingConstants.CENTER);
		lblTitle.setBounds(185, 104, 359, 85);
		frmIansSearchEngine.getContentPane().add(lblTitle);
		
		btnChooseFiles = new JButton("Choose Files");
		btnChooseFiles.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				btnChooseFilesActionPerformed(arg0);
			}
		});
		btnChooseFiles.setBounds(296, 219, 136, 43);
		frmIansSearchEngine.getContentPane().add(btnChooseFiles);
		
		scrollPane = new JScrollPane();
		scrollPane.setViewportBorder(null);
		scrollPane.setBounds(240, 279, 249, 78);
		frmIansSearchEngine.getContentPane().add(scrollPane);
		

		txtFileNames = new JTextArea();
		scrollPane.setViewportView(txtFileNames);
		scrollPane.setViewportBorder(null);
		scrollPane.setBorder(null);
		scrollPane.getViewport().setBorder(null);
		txtFileNames.setRows(3);
		txtFileNames.setLineWrap(true);
		txtFileNames.setEditable(false);
		txtFileNames.setBackground(SystemColor.menu);
		
		btnLoadEngine = new JButton("Load Engine");
		btnLoadEngine.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				try {
					btnLoadEngineActionPerformed(arg0);
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (GeneralSecurityException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		btnLoadEngine.setBounds(240, 368, 249, 61);
		frmIansSearchEngine.getContentPane().add(btnLoadEngine);
		
		chooser = new FileChooser();
	}
	
	private void btnLoadEngineActionPerformed(ActionEvent arg0) throws IOException, InterruptedException, GeneralSecurityException {
		if (chooser.files.isEmpty()) {
			throw new FileNotFoundException("No files were selected!");
		} else {
			Archiver archiver = ArchiverFactory.createArchiver("tar", "gz");
			UploadObject upload = new UploadObject();
			for (File f : chooser.files) {
				String newFolder = f.getName().substring(0, f.getName().indexOf(".tar.gz"));
				String fileName = f.getParent() + "/" + newFolder;
				File dest = new File(fileName);
				archiver.extract(f, dest);
				fetchFiles(dest, file -> {
					try {
						upload.uploadObject(bucketId, "data/" + file.getAbsolutePath().substring(file.getAbsolutePath().indexOf(newFolder), file.getAbsolutePath().length()).replace('\\', '/'), file.getAbsolutePath());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				});
				FileUtils.deleteDirectory(dest);
			}
			startInvertedIndexJob();
		}
	}
	
	private void btnChooseFilesActionPerformed(ActionEvent arg0) {
		try {
			chooser.pick_me();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (chooser.files.size() == 0) txtFileNames.setText("No file(s) selected!");
		else {
			StringBuilder sb = new StringBuilder();
			for (File f : chooser.files) {
				sb.append(f.getName() + '\n');
			}
			txtFileNames.setText(sb.toString());
		}
	}
	private void fetchFiles(File dir, Consumer<File> consumer) {
		if(dir.isDirectory()) {
			for (File file : dir.listFiles()) {
				fetchFiles(file, consumer);
			} 
		} else {
			consumer.accept(dir);
		}
	}
	
	private void startInvertedIndexJob() throws FileNotFoundException, IOException, InterruptedException, GeneralSecurityException {
		
		String projectId = "termproject-273618";
		String region = "us-west1";
		
		HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
		JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
		
		GoogleCredentials credential = GoogleCredentials.fromStream(new FileInputStream("src/main/termproject-273618-3c54da7f1d90.json"))
			.createScoped(Collections.singleton(DataprocScopes.CLOUD_PLATFORM));
		HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credential);
		
		Dataproc dataproc = new Dataproc.Builder(httpTransport, jsonFactory, requestInitializer)
				.setApplicationName("Inverted Index")
				.build();
		
		String jobId = "invert-index-job-" + UUID.randomUUID().toString();
		Job job = null;
		
		try {
			job = dataproc.projects().regions().jobs().submit("termproject-273618", "us-west1", new SubmitJobRequest()
					.setJob(new Job()
							.setReference(new JobReference()
									.setJobId(jobId))
							.setPlacement(new JobPlacement()
									.setClusterName("hadoop-cluster-1"))
							.setHadoopJob(new HadoopJob()
									.setMainClass("InvertedIndexJob")
									.setJarFileUris(ImmutableList.of("gs://dataproc-staging-us-west1-233708547529-zvfgtqye/JAR/invertedindex.jar"))
									.setArgs(ImmutableList.of(
											"gs://dataproc-staging-us-west1-233708547529-zvfgtqye/data",
											"gs://dataproc-staging-us-west1-233708547529-zvfgtqye/output")))))
					.execute();
			
		} catch (IOException e) {
			try {
				job = dataproc.projects().regions().jobs().get("termproject-273618", "us-west1", jobId).execute();
				System.out.println("Despite exception, job was verified submitted");
			} catch (IOException e2) {}
		}
		
		String details = "";
		String host = "";
		
		// grabs external-IP of master node to prepare for SSH
		job = dataproc.projects().regions().jobs().get(projectId, region, jobId).execute();
		Cluster cluster = dataproc.projects().regions().clusters().get(projectId, region, "hadoop-cluster-1").execute();
		InstanceGroupConfig iconfig = cluster.getConfig().getMasterConfig();
		Compute compute = new Compute.Builder(httpTransport, jsonFactory, requestInitializer)
				.setApplicationName("Inverted Index")
				.build();
		Compute.Instances.List instances = compute.instances().list(projectId, "us-west1-a");
		InstanceList list = instances.execute();
		// grabs NatIP of master node
		for (Instance instance : list.getItems()) {
			if (instance.getName().equals(iconfig.getInstanceNames().get(0))) {
				List<NetworkInterface> interfaceList = instance.getNetworkInterfaces();
				host = interfaceList.get(0).getAccessConfigs().get(0).getNatIP();
			}
		}
		
		while(true) {
			job = dataproc.projects().regions().jobs().get(projectId, region, jobId).execute();

			if (!job.getStatus().getState().equals(details)) {
				details = job.getStatus().getState();
				System.out.println(details);
				if (details.equals("DONE")) break;
				if (details.equals("ERROR")) {
					System.out.println(job.getStatus().getDetails());
					break;
				}
			} 
		}
		
		int exitCode = -1;
		if (details.equals("DONE")) {
			exitCode = commandSSH("ian_lundberg95", host, "gsutil rm gs://dataproc-staging-us-west1-233708547529-zvfgtqye/output/_SUCCESS"
					+ " && hadoop fs -getmerge gs://dataproc-staging-us-west1-233708547529-zvfgtqye/output/ ./output.txt"
					+ " && hadoop fs -copyFromLocal -f ./output.txt"
					+ " && hadoop fs -cp -f ./output.txt gs://dataproc-staging-us-west1-233708547529-zvfgtqye/output.txt");
			
			if (exitCode == 0) {
			
				DownloadObject download = new DownloadObject();
				download.downloadObject("dataproc-staging-us-west1-233708547529-zvfgtqye", "output.txt", Paths.get("Data/InvertedIndexData.txt"));
				
			} else { 
				System.out.println("Command exited with error code: " + exitCode);
			}
		}
	}
	
	private int commandSSH(String user, String host, String command) {
		
		String privKey = "src/main/key1";
		int status = -1;
		try {
			java.util.Properties config = new java.util.Properties();
			config.put("StrictHostKeyChecking", "no");
			JSch jsch = new JSch();
			jsch.addIdentity(privKey);
			Session session = jsch.getSession(user, host, 22);
			session.setConfig(config);
			
			session.connect();
			Channel channel = session.openChannel("exec");
			((ChannelExec) channel).setCommand(command);
			((ChannelExec) channel).setPty(false);
			channel.connect();
			
			while(!channel.isClosed()) {}
			status = channel.getExitStatus();
			
			channel.disconnect();
			session.disconnect();
			
		} catch (JSchException e) {
			e.printStackTrace();
		}
		return status;
	}
}
