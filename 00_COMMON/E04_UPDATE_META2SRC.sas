%macro update_META2SRC(metatbl=, srctbl=, tartbl=);
	/*%let metatbl=stg.rtmeta_bpe_rsme003_201507_201512;*/
	proc sort data=&metatbl. out=_metainfo_v2;  
		by realtime_seq; 
		where not missing(realtime_seq);
	run;
	/* check any duplicate record exists */
	proc sql noprint;
		select count(machine_duplicate_name) into :n_dup from _metainfo_v2
		where not missing(machine_duplicate_name);
	quit;
	/* Check whether the metainfo stored in excel exists in the file */
	proc sql noprint;
		select count(1) into :metacnt from _metainfo_v2 where not missing(metainfo_seq);
	quit;
%if %eval(&metacnt.) gt 0 %then %do;  
	data _null_;
		file "&dir_tmp./tmp3&st_TS..dat" lrecl=65535;
		put "data &tartbl.;";
		put "set &srctbl.";
	run;
	/* duplicate name - means orig_name is same as target SAS name */
	%if &n_dup. ne 0 %then %do; 
		data _null_;
			file "&dir_tmp./tmp3&st_TS..dat" mod;
			put "(rename=(";
		run;
		data _null_;
			file "&dir_tmp./tmp3&st_TS..dat" mod;
			set _metainfo_v2(where=(not missing(machine_duplicate_name)));
			put machine_duplicate_name "=" machine_name;	
		run;
		data _null_;
			file "&dir_tmp./tmp3&st_TS..dat" mod;
			put "));";
			put "length n_tmp 8;";
		run;
	%end;
	%else %do;
		data _null_;
			file "&dir_tmp./tmp3&st_TS..dat" mod;
			put ";";
			put "length n_tmp 8;";
		run;
	%end;
	data _null_;
		file "&dir_tmp./tmp3&st_TS..dat" mod;
		set _metainfo_v2;
		by realtime_seq;
		length _name $255 _tlabel $255;
		tmp = "'"||strip(machine_name)||"'n"; 
		/* create and format field based on target SAS name mentioned in excel file */
		if strip(orig_type) = "C" and strip(SAS_type) ="C" then do;
				put SAS_name "=strip(" tmp ");";
		end;
		if strip(orig_type) = "C" and strip(SAS_type) ="N" then do;
			put "n_tmp = .;";
			put 'n_tmp = input(strip(' tmp '), ' orig_format ');';
			put "_ERROR_=0;";
			put SAS_name '= n_tmp;';
			put 'format ' SAS_name SAS_format ';';
		end;
		/* label field based on orig_name */
		if not missing( orig_name ) then do;
			_tlabel="'"||tranwrd(strip(orig_name),"'","''")||"'";
			if not missing(SAS_name) then 	_name = SAS_name; 
			else 							_name = "'"||strip(machine_name)||"'n";
			put 'label ' _name  '=' _tlabel ";";
		end;
	run;
	data _null_;
		file "&dir_tmp./tmp3&st_TS..dat" mod;
		put "keep ";
	run;
	data _null_;
		file "&dir_tmp./tmp3&st_TS..dat" mod;
		set _metainfo_v2;
		by realtime_seq;
		put SAS_name;
		/* start - handle only realtime appeared fieldnames  - SAS_NAME is supposed to be blank */
		length txt $255;
		if n_ind_realtime_meta_only = 1 then do;
			txt = "'"||strip(machine_name)||"'n";
			put txt;			
		end;
		/* end - handle only realtime appeared fieldnames */
	run;	
	data _null_;
		file "&dir_tmp./tmp3&st_TS..dat" mod;
		put ";";
		
		put "RUN;";
	run;
	%put ---- NCB LOG ---- : Start converting field types based on input master.;
	%include "&dir_tmp./tmp3&st_TS..dat" /source2;
	%put ---- NCB LOG ---- : Finished converting field types based on input master.;
	/* alter length - format declared */
	data _metainfo_subset;
		set _metainfo_v2;
		where SAS_type="C" and not missing(SAS_format);
		length _tlen $50;
		_tlen=compress(SAS_format,"$.");
	run;
	data _null_;
		file "&dir_tmp./tmp3a&st_TS..dat";
		set _metainfo_subset end=eof;
		if _n_ =1 then do;
			put "proc sql noprint; alter table &tartbl. modify ";
		end;
		if not eof then do;
			put SAS_name "char(" _tlen "),";
		end;
		if eof then do;
			put SAS_name "char(" _tlen ");";
			put "quit;";
		end;
	run;
	%put ---- NCB LOG ---- : Start altering length of source file.;
	%include "&dir_tmp./tmp3a&st_TS..dat" /source2;
	%put ---- NCB LOG ---- : Finished altering length of source file.;

	/* Align the SAS output following the orignal txt source sequence */
	data _null_;
		file "&dir_tmp./tmp4&st_TS..dat"  lrecl=65535;
		put "data &tartbl.;";
		put "retain";
	run;
	data _null_;
		file "&dir_tmp./tmp4&st_TS..dat"  mod;
		set _metainfo_v2;
		by realtime_seq;
		put SAS_name;
/* start - handle only realtime appeared fieldnames  - SAS_NAME is supposed to be blank */
		length txt $255;
		if n_ind_realtime_meta_only = 1 then do;
			txt = "'"||strip(machine_name)||"'n";
			put txt;			
		end;
/* end - handle only realtime appeared fieldnames */
	run;
	data _null_;
		file "&dir_tmp./tmp4&st_TS..dat"  mod;
		put ";";
		put "set &tartbl.;";
		put "run;";
	run;
	%put ---- NCB LOG ---- : Start outputting SAS dataset based on original field sequence.;
	%include "&dir_tmp./tmp4&st_TS..dat" /source2;
	%put ---- NCB LOG ---- : Finished outputting SAS dataset based on original field sequence.;
%end;
%mend;
/*%update_META2SRC(src=bpe, srctbl=stg.bpe_csv_&st_RptMth., tartbl=raw.bpe_csv_&st_RptMth.);*/
/*
%update_META2SRC(metatbl=stg.rtmeta_bpe_rsme003_201507_201512, srctbl=stg.bpe_rsme003_201507_201512, tartbl=raw.bpe_rsme003_201507_201512);
*/
