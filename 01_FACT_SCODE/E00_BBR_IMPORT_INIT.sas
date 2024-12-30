/* ************************************************************************************************ */
/* To import the source file in SAS dataset in characters format for avoiding information loss.		*/
/* The SAS dataset in characters format is saved in libname "STG", we will have regular				*/
/* housekeeping on every 2 years in this libname "STG".												*/
/* ************************************************************************************************ */
/* Reference: https://communities.sas.com/t5/General-SAS-Programming/Creator-of-table/td-p/176453	*/


%macro gen_initial_bbr();
%if &flag_initial. = YES %then %do;

	filename bbrlist pipe "ls -l &dir_bbr."; * where directory is the path to your library;
	data bbr_fileinfo;
		infile bbrlist;
		input;
		st_yyyymm = strip(scan(_infile_,9," "));
		if compress(st_yyyymm,"0123456789") = "" and not missing(st_yyyymm)then do;
			output;
		end;
	run;

	%import_InputMaster(sheet=MetaInfo_BBR_Output, out=raw_bbr.meta_bbr_output_initial);
	proc sql noprint; select count(1) into :cnt from bbr_fileinfo; quit;

	%put ==>sense checking &cnt.;
	%do tmpcnt = 1 %to &cnt.;
		data _null_;
			set bbr_fileinfo(firstobs=&tmpcnt. obs=&tmpcnt.);
			call symput("folder",strip(st_yyyymm));
		run;
		filename bbrlist pipe "ls -l &dir_bbr./&folder.|grep -i 'BOCHK_DataOutput'|grep -i '.csv'"; * where directory is the path to your library;
		data bbr_fileinfo;
			infile bbrlist;
			input;
			csvname = strip(scan(_infile_,9," "));
			call symput("file",strip(csvname));
		run;
		/*%put ==> TESTLOG &folder. &file.; */
		%if &file. ne %then %do;
			%gen_SRC_IN_CHAR(orig_src=&dir_bbr./&folder./&file., orig_delim=",", sas_tblname=stg_bbr.bbr_output_&folder., realtime_meta=work.rtmeta_bbr_output_&folder.); 
			%trim_SRC_FIELDNAME(proposed_meta=raw_bbr.meta_bbr_output_initial, realtime_meta=work.rtmeta_bbr_output_&folder., final_meta=stg_bbr.rtmeta_bbr_output_&folder., st_dSpecific=&folder.);
			%update_META2SRC(metatbl=stg_bbr.rtmeta_bbr_output_&folder., srctbl=stg_bbr.bbr_output_&folder., tartbl=raw_bbr.bbr_output_&folder.);
			%update_ManualAdj(table_name=BBR_Output, src=raw_bbr.bbr_output_&folder., tar=raw_bbr.bbr_output_&folder., st_date=&folder.);
		%end;
	%end;
%end;
%mend;
%exportlog(ind=START, path=&dir_log., name=BBR_OUTPUT_INIT_LOAD_&st_cltinfo.);
%import_Format(src=BBR);
%gen_initial_bbr;
%exportlog(ind=STOP);

