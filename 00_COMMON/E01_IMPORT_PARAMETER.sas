
%macro import_InputMaster(sheet=,out=);
	proc import 
		datafile="&dir_parm./BID_input_master.xlsx" 
		out=&out.
		dbms=xlsx replace;
		sheet="&sheet.";
	run;
	data &out.;
		set &out.;
		array n{*} _numeric_;
		array c{*} _character_;
		if sum(of n[*]) = . and compress(catt(of c[*]))="" then delete;
	run;
%mend;

%macro import_Format(src=);
	/*TYPE - specifies a character variable that indicates the type of format. Possible values are as follows:*/
	/*C	- specifies a character format.*/
	/*I	- specifies a numeric informat.*/
	/*J	- specifies a character informat.*/
	/*N	- specifies a numeric format (excluding pictures).*/
	/*P	- specifies a picture format.*/
	data fmtdataset;
		set raw.custom_informat;
		if effect_dt_from <= &dt_RptMth. <= effect_dt_to;
		if source_system in ("&src.");
		fmtname 	= strip(SAS_informat);
		type 		= strip(SAS_informat_type);
		start		= strip(orig_value);
		label		= strip(SAS_value);
		seq			= _N_;
		if strip(orig_value)="other" then do;
			start="**OTHER**";
			if substr(reverse(strip(label)),1,1)="." then HLO="OF";
			else HLO="O";
		end;
	run;
	proc sql noprint;
		select count(1) into :cnt from fmtdataset;
	quit;
	%if &cnt. ne 0 and &cnt. ne  %then %do;
		proc sort data=fmtdataset out=fmt nodupkey; by fmtname start seq; run;
		proc format cntlin=fmt; run;
	%end;
%mend;

/* ************************************************************************* */
/* ************************************************************************* */
/* START - Read all custom format into SAS session							 */
%import_InputMaster(sheet=Custom_Informat,			out=raw_oth.custom_informat);
%import_InputMaster(sheet=Manual_Adj, 				out=raw_oth.manual_adj);
%import_InputMaster(sheet=Lookup_PD_Master_Scale,	out=raw_oth.pd_master_scale);
%import_InputMaster(sheet=Lookup_CapitialIQ_RMP,	out=raw_oth.rmp_sp_mapping);
%import_InputMaster(sheet=Lookup_Bloomberg_Rating,	out=raw_oth.bloomberg_rating);
%import_Format(src=ALL);

/* For example and ad-hoc usage 
%import_InputMaster(sheet=MetaInfo_BPE_RSME003, out=raw.meta_bpe_rsme003_initial);
%import_Format(src=SAM);
%import_Format(src=RMP);
proc format cntlout=test; run;
*/

/* END - Read all custom format into SAS session */
/* ************************************************************************* */





