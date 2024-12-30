/* ************************************************************************************************ */
/* To import the source file in SAS dataset in characters format for avoiding information loss.		*/
/* The SAS dataset in characters format is saved in libname "STG", we will have regular				*/
/* housekeeping on every 2 years in this libname "STG".												*/
/* ************************************************************************************************ */
%macro gen_BBR();
%let rawfile=bbr_output_&st_rptmth.;

%if %SYSFUNC(exist(raw_bbr.&rawfile.)) = 1 %then %do;
	%put ---- NCB LOG ---- : !! raw_bbr.&rawfile. is already existed.;
%end;
%else %do;
	x "cd &dir_bbr.";
	x "ls -al > filelist.txt";
	data bbr_fileinfo;
		length st_yyyymm $6;
		infile "&dir_bbr./filelist.txt" 
		DLM='7F'x MISSOVER DSD ;
		INPUT  text   : $CHAR255.;
		text1 = reverse(strip(text));
		st_yyyymm = reverse(substr(text1,1,6));
		if compress(st_yyyymm,"0123456789") = "" then do;
			output;
		end;
	run;
	proc sql noprint; select count(1) into :cnt from bbr_fileinfo where strip(st_yyyymm)="&st_RptMth."; quit;
	%if &cnt. ne 1 %then %do;
		%put ---- NCB LOG ---- : !! BBR folder of &st_RptMth. does not exist.;
	%end;
	%else %do;
		x "cd &dir_bbr./&st_RptMth.";
		x "ls -al > filelist.txt";
		%let file=;
		data _test;
			infile "&dir_bbr./&st_RptMth./filelist.txt" 
			DLM='7F'x MISSOVER DSD ;
			INPUT  text   : $CHAR200.;
			if find(text ,'BOCHK_DataOutput','i') and find(upcase(text),'.CSV','i') then do;
				index=index(reverse(strip(text))," ");
				csvname=reverse(substr(reverse(strip(text)),1,index-1));
				call symput("file",strip(csvname));
			end;
		run;
		%if &file. ne %then %do;
			%import_InputMaster(sheet=MetaInfo_BBR_Output, out=raw_bbr.meta_bbr_output_&st_RptMth.);
			%gen_SRC_IN_CHAR(orig_src=&dir_bbr./&st_RptMth./&file., orig_delim=",", sas_tblname=stg_bbr.bbr_output_&st_RptMth., realtime_meta=work.rtmeta_bbr_output_&st_RptMth.); 
			%trim_SRC_FIELDNAME(proposed_meta=raw_bbr.meta_bbr_output_&st_RptMth., realtime_meta=work.rtmeta_bbr_output_&st_RptMth., final_meta=stg_bbr.rtmeta_bbr_output_&st_RptMth., st_dSpecific=&st_RptMth.);
			%update_META2SRC(metatbl=stg_bbr.rtmeta_bbr_output_&st_RptMth., srctbl=stg_bbr.bbr_output_&st_RptMth., tartbl=raw_bbr.bbr_output_&st_RptMth.);
			%update_ManualAdj(table_name=BBR_Output, src=raw_bbr.bbr_output_&st_RptMth., tar=raw_bbr.bbr_output_&st_RptMth., st_date=&st_RptMth.);
		%end;
	%end;
%end;
%mend;

%exportlog(ind=START, path=&dir_log., name=BBR_OUTPUT_&st_RptMth._LOAD_&st_cltinfo.);
%import_Format(src=BBR);
%gen_BBR;
%exportlog(ind=STOP);
