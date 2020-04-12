package main;

import java.awt.EventQueue;
import java.awt.Font;
import java.awt.SystemColor;
import java.awt.Window.Type;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingConstants;
import javax.swing.table.DefaultTableModel;

import org.apache.commons.io.FileUtils;
import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;
import org.unix4j.Unix4j;
import org.unix4j.line.Line;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.InstanceList;
import com.google.api.services.compute.model.NetworkInterface;
import com.google.api.services.dataproc.Dataproc;
import com.google.api.services.dataproc.DataprocScopes;
import com.google.api.services.dataproc.model.Cluster;
import com.google.api.services.dataproc.model.HadoopJob;
import com.google.api.services.dataproc.model.InstanceGroupConfig;
import com.google.api.services.dataproc.model.Job;
import com.google.api.services.dataproc.model.JobPlacement;
import com.google.api.services.dataproc.model.JobReference;
import com.google.api.services.dataproc.model.SubmitJobRequest;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableList;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;


public class EngineGUI {

	private JFrame frmIansSearchEngine;
	private JLabel lblTitle;
	private JButton btnChooseFiles;
	private JTextArea txtFileNames;
	private JScrollPane fileScrollPane;
	private JButton btnLoadEngine;
	private JTextField txtSearch;
	private JTable tblInvertedIndicies;
	private FileChooser chooser;
	
	private List<Line> indicesResult;
	
	private final String bucketId = "dataproc-staging-us-west1-233708547529-zvfgtqye";
	private JLabel lblAnpercant;
	private JLabel lblIndiciesLoaded;
	private JLabel lblSelectAction;
	private JButton btnSearchForTerm;
	private JLabel lblEnterTerm;
	private JScrollPane tableScrollPane;
	private JLabel lblSearched;
	private JButton btnBack;
	private JLabel lblLoaded;
	private JButton btnSearchIndicies;


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
	@SuppressWarnings("serial")
	private void initialize() {
		frmIansSearchEngine = new JFrame();
		frmIansSearchEngine.setTitle("Ian's Search Engine");
		frmIansSearchEngine.setType(Type.UTILITY);
		frmIansSearchEngine.setResizable(false);
		frmIansSearchEngine.setBounds(100, 100, 735, 553);
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
		
		fileScrollPane = new JScrollPane();
		fileScrollPane.setViewportBorder(null);
		fileScrollPane.setBounds(240, 279, 249, 78);
		frmIansSearchEngine.getContentPane().add(fileScrollPane);
		

		txtFileNames = new JTextArea();
		fileScrollPane.setViewportView(txtFileNames);
		fileScrollPane.setViewportBorder(null);
		fileScrollPane.setBorder(null);
		fileScrollPane.getViewport().setBorder(null);
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
		
		btnSearchIndicies = new JButton("Search");
		btnSearchIndicies.setVisible(false);
		btnSearchIndicies.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				indicesResult = Unix4j.fromFile("Data/InvertedIndexData.txt").grep("^" + txtSearch.getText()).toLineList();
				displaySearchResults(indicesResult, txtSearch.getText());
				btnSearchIndicies.setVisible(false);
				txtSearch.setVisible(false);
				lblEnterTerm.setVisible(false);
				
				tableScrollPane.setVisible(true);
				lblSearched.setText("You searched for the term: " + txtSearch.getText());
				lblSearched.setVisible(true);
				btnBack.setVisible(true);
			}
		});
		btnSearchIndicies.setBounds(240, 280, 249, 61);
		frmIansSearchEngine.getContentPane().add(btnSearchIndicies);
		
		txtSearch = new JTextField();
		txtSearch.setActionCommand("");
		txtSearch.setVisible(false);
		txtSearch.setBounds(226, 215, 277, 20);
		frmIansSearchEngine.getContentPane().add(txtSearch);
		txtSearch.setColumns(10);
		
		tableScrollPane = new JScrollPane();
		tableScrollPane.setVisible(false);
		tableScrollPane.setBounds(42, 219, 644, 248);
		frmIansSearchEngine.getContentPane().add(tableScrollPane);
		
		tblInvertedIndicies = new JTable();
		tableScrollPane.setViewportView(tblInvertedIndicies);
		tblInvertedIndicies.setModel(new DefaultTableModel(
			new Object[][] {
			},
			new String[] {
				"Term", "Document", "Frequency"
			}
		) {
			@SuppressWarnings("rawtypes")
			Class[] columnTypes = new Class[] {
				String.class, String.class, String.class
			};
			@SuppressWarnings({ "unchecked", "rawtypes" })
			public Class getColumnClass(int columnIndex) {
				return columnTypes[columnIndex];
			}
			boolean[] columnEditables = new boolean[] {
				false, false, false
			};
			public boolean isCellEditable(int row, int column) {
				return columnEditables[column];
			}
		});
		
		lblLoaded = new JLabel("Engine was loaded");
		lblLoaded.setVisible(false);
		lblLoaded.setHorizontalAlignment(SwingConstants.CENTER);
		lblLoaded.setFont(new Font("Tahoma", Font.BOLD, 24));
		lblLoaded.setBounds(185, 104, 359, 85);
		frmIansSearchEngine.getContentPane().add(lblLoaded);
		
		lblAnpercant = new JLabel("&");
		lblAnpercant.setVisible(false);
		lblAnpercant.setHorizontalAlignment(SwingConstants.CENTER);
		lblAnpercant.setFont(new Font("Tahoma", Font.BOLD, 24));
		lblAnpercant.setBounds(185, 136, 359, 85);
		frmIansSearchEngine.getContentPane().add(lblAnpercant);
		
		lblIndiciesLoaded = new JLabel("Inverted indicies were constructed successfully!");
		lblIndiciesLoaded.setVisible(false);
		lblIndiciesLoaded.setHorizontalAlignment(SwingConstants.CENTER);
		lblIndiciesLoaded.setFont(new Font("Tahoma", Font.BOLD, 24));
		lblIndiciesLoaded.setBounds(42, 168, 644, 85);
		frmIansSearchEngine.getContentPane().add(lblIndiciesLoaded);
		
		lblSelectAction = new JLabel("Please Select Action");
		lblSelectAction.setVisible(false);
		lblSelectAction.setHorizontalAlignment(SwingConstants.CENTER);
		lblSelectAction.setFont(new Font("Tahoma", Font.BOLD, 24));
		lblSelectAction.setBounds(185, 279, 359, 85);
		frmIansSearchEngine.getContentPane().add(lblSelectAction);
		
		btnSearchForTerm = new JButton("Search for Term");
		btnSearchForTerm.setVisible(false);
		btnSearchForTerm.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				btnSearchForTerm.setVisible(false);
				lblSelectAction.setVisible(false);
				lblIndiciesLoaded.setVisible(false);
				lblAnpercant.setVisible(false);
				lblLoaded.setVisible(false);
				
				lblEnterTerm.setVisible(true);
				txtSearch.setVisible(true);
				btnSearchIndicies.setVisible(true);
				
			}
		});
		btnSearchForTerm.setBounds(240, 368, 249, 61);
		frmIansSearchEngine.getContentPane().add(btnSearchForTerm);
		
		lblEnterTerm = new JLabel("Enter Your Search Term");
		lblEnterTerm.setVisible(false);
		lblEnterTerm.setHorizontalAlignment(SwingConstants.CENTER);
		lblEnterTerm.setFont(new Font("Tahoma", Font.BOLD, 24));
		lblEnterTerm.setBounds(185, 104, 359, 85);
		frmIansSearchEngine.getContentPane().add(lblEnterTerm);
		
		lblSearched = new JLabel("");
		lblSearched.setVisible(false);
		lblSearched.setBounds(67, 114, 477, 43);
		frmIansSearchEngine.getContentPane().add(lblSearched);
		
		btnBack = new JButton("Go Back");
		btnBack.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				tableScrollPane.setVisible(false);
				lblSearched.setVisible(false);
				btnBack.setVisible(false);
				
				btnSearchIndicies.setVisible(true);
				txtSearch.setVisible(true);
				lblEnterTerm.setVisible(true);
				tableScrollPane.getVerticalScrollBar().setValue(0);
				
			}
		});
		btnBack.setVisible(false);
		btnBack.setBounds(580, 31, 106, 43);
		frmIansSearchEngine.getContentPane().add(btnBack);
		tblInvertedIndicies.getColumnModel().getColumn(0).setResizable(false);
		tblInvertedIndicies.getColumnModel().getColumn(1).setResizable(false);
		tblInvertedIndicies.getColumnModel().getColumn(2).setResizable(false);
		
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
				else if (details.equals("ERROR")) {
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
				lblTitle.setVisible(false);
				btnChooseFiles.setVisible(false);
				btnLoadEngine.setVisible(false);
				fileScrollPane.setVisible(false);
				
				lblLoaded.setVisible(true);
				lblAnpercant.setVisible(true);
				lblIndiciesLoaded.setVisible(true);
				lblSelectAction.setVisible(true);
				btnSearchForTerm.setVisible(true);
				
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
			
			System.out.println("\nMerging...");
			while(!channel.isClosed()) {}
			status = channel.getExitStatus();
			System.out.println("\nDone.");
			channel.disconnect();
			session.disconnect();
			
		} catch (JSchException e) {
			e.printStackTrace();
		}
		return status;
	}
	
	private void displaySearchResults(List<Line> lines, String search) {
		HashMap<String, HashMap<String, String>> fmap = new HashMap<String, HashMap<String, String>>();
		for(Line line : lines) {
			String strLine = line.getContent();
			if(!strLine.equals("")) {
				if (strLine.substring(0, strLine.indexOf('{')-1).trim().equals(search)) {
					String term = strLine.substring(0, strLine.indexOf('{')-1).trim();
					HashMap<String, String> map = new HashMap<String, String>();
					strLine = strLine.substring(strLine.indexOf('{')+1, strLine.lastIndexOf('}'));
					String[] values = strLine.split(",");
					for(String s : values)
						map.put(s.substring(0, s.indexOf('=')).trim(), s.substring(s.indexOf('=')+1, s.length()).trim());
					fmap.put(term, map);
				}
			}
		}
		
		DefaultTableModel model = (DefaultTableModel)tblInvertedIndicies.getModel();
		int rows = model.getRowCount();
		for(int i = rows - 1; i >= 0; i--) {
			model.removeRow(i);
		}
		
		for (String key : fmap.keySet()) {
			HashMap<String, String> secondMap = fmap.get(key);
			for(String secondKey : secondMap.keySet()) {
				String[] row = { key, secondKey, secondMap.get(secondKey) };
				model.addRow(row);
			}
		}
	}
}
