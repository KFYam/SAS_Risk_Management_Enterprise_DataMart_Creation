/* ************************************************************************************************ */
/* To import the source file in SAS dataset in characters format for avoiding information loss.		*/
/* The SAS dataset in characters format is saved in libname "STG", we will have regular				*/
/* housekeeping on every 2 years in this libname "STG".												*/
/* ************************************************************************************************ */

/* Split dataset */
%macro split_Table(src=,prefix=,id=,id_type=);
/*	%let src=raw_bpe.BPE_RSME003_200908_200912;*/
/*	%let prefix=raw_bpe.BPE_RSME003;*/
/*	%let id=MonthID2;*/
	%if &id_type. ne C %then %do;
		proc sql noprint;
			create table key as	
				select &id.,count(1) as count from &src. group by &id.;
		quit;
		data _null_;
			file "&dir_tmp./tmp5&st_TS..dat" lrecl=65535;
			set key;
			tblname="&prefix._"||strip(put(sum(200000,&id.),z6.));
			put "data " tblname ";";
			put "set &src.;"; 
			put "where &id. = " &id. ";";
			put "run;";
		run;
		%include "&dir_tmp./tmp5&st_TS..dat" /source2; 
	%end;
	%if &id_type. eq C %then %do;
		proc sql noprint;
			create table key as	
			select &id.,input(&id.,best32.) as n_monthid,count(1) as count 
				from &src. 
				where not missing(&id.)
				group by &id.;
		quit;
		data _null_;
			file "&dir_tmp./tmp5&st_TS..dat" lrecl=65535;
			length tblname whereclause $255;
			set key;
			tblname="&prefix._"||strip(put(sum(200000,n_monthid),z6.));
			whereclause="where &id. = '"||strip(&id.)||"';";
			put "data " tblname ";";
			put "set &src.;"; 
			put whereclause;
			put "run;";
		run;
		%include "&dir_tmp./tmp5&st_TS..dat" /source2; 
	%end;
%mend;

%macro submod_bpe();
	%import_InputMaster(sheet=MetaInfo_BPE, 				out=raw_bpe.meta_bpe_initial);
	%trim_SRC_FIELDNAME(proposed_meta=raw_b
