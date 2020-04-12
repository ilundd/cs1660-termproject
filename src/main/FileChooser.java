package main;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

import javax.swing.JFileChooser;
import javax.swing.filechooser.FileNameExtensionFilter;

public class FileChooser {
	JFileChooser fileDialog = new JFileChooser();
	ArrayList<File> files = new ArrayList<File>();
	
	public void pick_me() throws FileNotFoundException {
		fileDialog.setAcceptAllFileFilterUsed(false);
		fileDialog.setCurrentDirectory(new File(System.getProperty("user.dir")));
		fileDialog.setFileFilter(new FileNameExtensionFilter("Gzip file (*.gz)", "gz"));
		fileDialog.setMultiSelectionEnabled(true);
		
		if (fileDialog.showOpenDialog(null) == JFileChooser.APPROVE_OPTION) {
			for(File f : fileDialog.getSelectedFiles()) {
				if (!files.contains(f)) files.add(f);
			}
		} else {
			files.clear();
		}
	}
}
