package com.ruckuswireless.pentaho.kafka.consumer;

import java.util.Properties;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

/**
 * UI for the Kafka Consumer step
 * 
 * @author Michael Spector
 */
public class KafkaConsumerDialog extends BaseStepDialog implements StepDialogInterface {

	private KafkaConsumerMeta consumerMeta;
	private TextVar wTopicName;
	private TextVar wFieldName;
	private TableView wProps;
	private TextVar wLimit;
	private TextVar wTimeout;
	private Button wStopOnEmptyTopic;
	
	public KafkaConsumerDialog(Shell parent, Object in, TransMeta tr, String sname) {
		super(parent, (BaseStepMeta) in, tr, sname);
		consumerMeta = (KafkaConsumerMeta) in;
	}

	public KafkaConsumerDialog(Shell parent, BaseStepMeta baseStepMeta, TransMeta transMeta, String stepname) {
		super(parent, baseStepMeta, transMeta, stepname);
		consumerMeta = (KafkaConsumerMeta) baseStepMeta;
	}

	public KafkaConsumerDialog(Shell parent, int nr, BaseStepMeta in, TransMeta tr) {
		super(parent, nr, in, tr);
		consumerMeta = (KafkaConsumerMeta) in;
	}

	public String open() {
		Shell parent = getParent();
		Display display = parent.getDisplay();

		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
		props.setLook(shell);
		setShellImage(shell, consumerMeta);

		ModifyListener lsMod = new ModifyListener() {
			public void modifyText(ModifyEvent e) {
				consumerMeta.setChanged();
			}
		};
		changed = consumerMeta.hasChanged();

		FormLayout formLayout = new FormLayout();
		formLayout.marginWidth = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		shell.setText(Messages.getString("KafkaConsumerDialog.Shell.Title"));

		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		// Step name
		wlStepname = new Label(shell, SWT.RIGHT);
		wlStepname.setText(Messages.getString("KafkaConsumerDialog.StepName.Label"));
		props.setLook(wlStepname);
		fdlStepname = new FormData();
		fdlStepname.left = new FormAttachment(0, 0);
		fdlStepname.right = new FormAttachment(middle, -margin);
		fdlStepname.top = new FormAttachment(0, margin);
		wlStepname.setLayoutData(fdlStepname);
		wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wStepname);
		wStepname.addModifyListener(lsMod);
		fdStepname = new FormData();
		fdStepname.left = new FormAttachment(middle, 0);
		fdStepname.top = new FormAttachment(0, margin);
		fdStepname.right = new FormAttachment(100, 0);
		wStepname.setLayoutData(fdStepname);
		Control lastControl = wStepname;

		// Topic name
		Label wlTopicName = new Label(shell, SWT.RIGHT);
		wlTopicName.setText(Messages.getString("KafkaConsumerDialog.TopicName.Label"));
		props.setLook(wlTopicName);
		FormData fdlTopicName = new FormData();
		fdlTopicName.top = new FormAttachment(lastControl, margin);
		fdlTopicName.left = new FormAttachment(0, 0);
		fdlTopicName.right = new FormAttachment(middle, -margin);
		wlTopicName.setLayoutData(fdlTopicName);
		wTopicName = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wTopicName);
		wTopicName.addModifyListener(lsMod);
		FormData fdTopicName = new FormData();
		fdTopicName.top = new FormAttachment(lastControl, margin);
		fdTopicName.left = new FormAttachment(middle, 0);
		fdTopicName.right = new FormAttachment(100, 0);
		wTopicName.setLayoutData(fdTopicName);
		lastControl = wTopicName;

		// Field name
		Label wlFieldName = new Label(shell, SWT.RIGHT);
		wlFieldName.setText(Messages.getString("KafkaConsumerDialog.FieldName.Label"));
		props.setLook(wlFieldName);
		FormData fdlFieldName = new FormData();
		fdlFieldName.top = new FormAttachment(lastControl, margin);
		fdlFieldName.left = new FormAttachment(0, 0);
		fdlFieldName.right = new FormAttachment(middle, -margin);
		wlFieldName.setLayoutData(fdlFieldName);
		wFieldName = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wFieldName);
		wFieldName.addModifyListener(lsMod);
		FormData fdFieldName = new FormData();
		fdFieldName.top = new FormAttachment(lastControl, margin);
		fdFieldName.left = new FormAttachment(middle, 0);
		fdFieldName.right = new FormAttachment(100, 0);
		wFieldName.setLayoutData(fdFieldName);
		lastControl = wFieldName;

		// Messages limit
		Label wlLimit = new Label(shell, SWT.RIGHT);
		wlLimit.setText(Messages.getString("KafkaConsumerDialog.Limit.Label"));
		props.setLook(wlLimit);
		FormData fdlLimit = new FormData();
		fdlLimit.top = new FormAttachment(lastControl, margin);
		fdlLimit.left = new FormAttachment(0, 0);
		fdlLimit.right = new FormAttachment(middle, -margin);
		wlLimit.setLayoutData(fdlLimit);
		wLimit = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wLimit);
		wLimit.addModifyListener(lsMod);
		FormData fdLimit = new FormData();
		fdLimit.top = new FormAttachment(lastControl, margin);
		fdLimit.left = new FormAttachment(middle, 0);
		fdLimit.right = new FormAttachment(100, 0);
		wLimit.setLayoutData(fdLimit);
		lastControl = wLimit;

		// Read timeout
		Label wlTimeout = new Label(shell, SWT.RIGHT);
		wlTimeout.setText(Messages.getString("KafkaConsumerDialog.Timeout.Label"));
		props.setLook(wlTimeout);
		FormData fdlTimeout = new FormData();
		fdlTimeout.top = new FormAttachment(lastControl, margin);
		fdlTimeout.left = new FormAttachment(0, 0);
		fdlTimeout.right = new FormAttachment(middle, -margin);
		wlTimeout.setLayoutData(fdlTimeout);
		wTimeout = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wTimeout);
		wTimeout.addModifyListener(lsMod);
		FormData fdTimeout = new FormData();
		fdTimeout.top = new FormAttachment(lastControl, margin);
		fdTimeout.left = new FormAttachment(middle, 0);
		fdTimeout.right = new FormAttachment(100, 0);
		wTimeout.setLayoutData(fdTimeout);
		lastControl = wTimeout;

		Label wlStopOnEmptyTopic = new Label(shell, SWT.RIGHT);
		wlStopOnEmptyTopic.setText(Messages.getString("KafkaConsumerDialog.StopOnEmpty.Label"));
		props.setLook(wlStopOnEmptyTopic);
		FormData fdlStopOnEmptyTopic = new FormData();
		fdlStopOnEmptyTopic.top = new FormAttachment(lastControl, margin);
		fdlStopOnEmptyTopic.left = new FormAttachment(0, 0);
		fdlStopOnEmptyTopic.right = new FormAttachment(middle, -margin);
		wlStopOnEmptyTopic.setLayoutData(fdlStopOnEmptyTopic);
		wStopOnEmptyTopic = new Button(shell, SWT.CHECK | SWT.LEFT | SWT.BORDER);
		props.setLook(wStopOnEmptyTopic);
		FormData fdStopOnEmptyTopic = new FormData();
		fdStopOnEmptyTopic.top = new FormAttachment(lastControl, margin);
		fdStopOnEmptyTopic.left = new FormAttachment(middle, 0);
		fdStopOnEmptyTopic.right = new FormAttachment(100, 0);
		wStopOnEmptyTopic.setLayoutData(fdStopOnEmptyTopic);
		lastControl = wStopOnEmptyTopic;

		// Buttons
		wOK = new Button(shell, SWT.PUSH);
		wOK.setText(BaseMessages.getString("System.Button.OK")); //$NON-NLS-1$
		wCancel = new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString("System.Button.Cancel")); //$NON-NLS-1$

		setButtonPositions(new Button[] { wOK, wCancel }, margin, null);

		// Kafka properties
		ColumnInfo[] colinf = new ColumnInfo[] {
				new ColumnInfo(Messages.getString("KafkaConsumerDialog.TableView.NameCol.Label"),
						ColumnInfo.COLUMN_TYPE_TEXT, false),
				new ColumnInfo(Messages.getString("KafkaConsumerDialog.TableView.ValueCol.Label"),
						ColumnInfo.COLUMN_TYPE_TEXT, false), };

		wProps = new TableView(transMeta, shell, SWT.FULL_SELECTION | SWT.MULTI, colinf, 1, lsMod, props);
		FormData fdProps = new FormData();
		fdProps.top = new FormAttachment(lastControl, margin * 2);
		fdProps.bottom = new FormAttachment(wOK, -margin * 2);
		fdProps.left = new FormAttachment(0, 0);
		fdProps.right = new FormAttachment(100, 0);
		wProps.setLayoutData(fdProps);

		// Add listeners
		lsCancel = new Listener() {
			public void handleEvent(Event e) {
				cancel();
			}
		};
		lsOK = new Listener() {
			public void handleEvent(Event e) {
				ok();
			}
		};
		wCancel.addListener(SWT.Selection, lsCancel);
		wOK.addListener(SWT.Selection, lsOK);

		lsDef = new SelectionAdapter() {
			public void widgetDefaultSelected(SelectionEvent e) {
				ok();
			}
		};
		wStepname.addSelectionListener(lsDef);
		wTopicName.addSelectionListener(lsDef);
		wFieldName.addSelectionListener(lsDef);
		wLimit.addSelectionListener(lsDef);
		wTimeout.addSelectionListener(lsDef);
		wStopOnEmptyTopic.addSelectionListener(lsDef);

		// Detect X or ALT-F4 or something that kills this window...
		shell.addShellListener(new ShellAdapter() {
			public void shellClosed(ShellEvent e) {
				cancel();
			}
		});

		// Set the shell size, based upon previous time...
		setSize(shell, 400, 350, true);

		getData(consumerMeta, true);
		consumerMeta.setChanged(changed);

		shell.open();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}
		return stepname;
	}

	/**
	 * Copy information from the meta-data input to the dialog fields.
	 */
	private void getData(KafkaConsumerMeta consumerMeta, boolean copyStepname) {
		if (copyStepname) {
			wStepname.setText(stepname);
		}
		wTopicName.setText(Const.NVL(consumerMeta.getTopic(), ""));
		wFieldName.setText(Const.NVL(consumerMeta.getField(), ""));
		wLimit.setText(Long.toString(consumerMeta.getLimit()));
		wTimeout.setText(Long.toString(consumerMeta.getTimeout()));
		wStopOnEmptyTopic.setSelection(consumerMeta.isStopOnEmptyTopic());

		Properties kafkaProperties = consumerMeta.getKafkaProperties();
		for (int i = 0; i < KafkaConsumerMeta.KAFKA_PROPERTIES_NAMES.length; ++i) {
			String propName = KafkaConsumerMeta.KAFKA_PROPERTIES_NAMES[i];
			String value = kafkaProperties.getProperty(propName);
			TableItem item = new TableItem(wProps.table, i > 1 ? SWT.BOLD : SWT.NONE);
			int colnr = 1;
			item.setText(colnr++, Const.NVL(propName, ""));
			String defaultValue = KafkaConsumerMeta.KAFKA_PROPERTIES_DEFAULTS.get(propName);
			if (defaultValue == null) {
				defaultValue = "(default)";
			}
			item.setText(colnr++, Const.NVL(value, defaultValue));
		}
		wProps.removeEmptyRows();
		wProps.setRowNums();
		wProps.optWidth(true);

		wStepname.selectAll();
	}

	private void cancel() {
		stepname = null;
		consumerMeta.setChanged(changed);
		dispose();
	}

	/**
	 * Copy information from the dialog fields to the meta-data input
	 */
	private void setData(KafkaConsumerMeta consumerMeta) {
		consumerMeta.setTopic(wTopicName.getText());
		consumerMeta.setField(wFieldName.getText());
		consumerMeta.setLimit(Const.toLong(wLimit.getText(), 0));
		consumerMeta.setTimeout(Const.toLong(wTimeout.getText(), 0));
		consumerMeta.setStopOnEmptyTopic(wStopOnEmptyTopic.getSelection());

		Properties kafkaProperties = consumerMeta.getKafkaProperties();
		int nrNonEmptyFields = wProps.nrNonEmpty();
		for (int i = 0; i < nrNonEmptyFields; i++) {
			TableItem item = wProps.getNonEmpty(i);
			int colnr = 1;
			String name = item.getText(colnr++);
			String value = item.getText(colnr++).trim();
			if (value.length() > 0 && !"(default)".equals(value)) {
				kafkaProperties.put(name, value);
			} else {
				kafkaProperties.remove(name);
			}
		}
		wProps.removeEmptyRows();
		wProps.setRowNums();
		wProps.optWidth(true);

		consumerMeta.setChanged();
	}

	private void ok() {
		if (Const.isEmpty(wStepname.getText())) {
			return;
		}
		setData(consumerMeta);
		stepname = wStepname.getText();
		dispose();
	}
}
