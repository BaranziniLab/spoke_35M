from s3_utility import get_saved_compounds_with_pagerank, read_pickle_file_from_s3
import boto3
import pickle
import sys
import time


compound_type = sys.argv[1]
sample = sys.argv[2]
sel_sheet_index = int(sys.argv[3])
data_path = sys.argv[4]

pvalue_thresh = 0.05
bucket_name = 'ic-spoke'
sheet_name_list = ["Without outlier", "Without outlier-MS treated", "Without outlier-MS not treated",
              "With outlier", "With outlier-MStreated", "With outlier-MS not treated"]
compounds_with_pagerank = get_saved_compounds_with_pagerank()


def main():
    start_time = time.time()
    spoke_embedding_dict = get_spoke_embedding(compound_type, sample, sel_sheet_index, data_path, pvalue_thresh=pvalue_thresh)
    binary_data = pickle.dumps(spoke_embedding_dict)
    del(spoke_embedding_dict)
    filename = "spoke_embedding_for_IMSMS_" + compound_type + "_compounds_" + sample + "_sample_" + "sheet_index_" + str(sel_sheet_index) + "_dict.pickle" 
    s3_client = boto3.client('s3')    
    object_key = 'spoke35M/spoke35M_iMSMS_embedding/{}'.format(filename)
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=binary_data)
    s3_client.close()
    del(binary_data)
    print("Completed in {} min".format(round((time.time() - start_time)/60, 2)))
    

def get_spoke_embedding(compound_type, sample, sel_sheet_index, data_path, pvalue_thresh=0.05):
    GLM_significant_compounds_mapped_to_SPOKE = get_significant_compounds_with_disease_association(compound_type, sample, sel_sheet_index, data_path, pvalue_thresh=pvalue_thresh)
    spoke_embedding_dict = {}
    spoke_vector = 0
    entry_point_count = 0
    for index, row in GLM_significant_compounds_mapped_to_SPOKE.iterrows():
        compound_id = "Compound:" + row["spoke_identifer"]
        if compound_id in compounds_with_pagerank:
            entry_point_count += 1
            object_key = "spoke35M/spoke35M_converged_ppr/" + compound_id + "_dict.pickle"
            spoke_embedding_data = read_pickle_file_from_s3(bucket_name, object_key)
            spoke_vector += row["disease_coeff"]*spoke_embedding_data["embedding"]
            del(spoke_embedding_data)
    spoke_embedding_dict["compound_type"] = compound_type
    spoke_embedding_dict["sample"] = sample
    spoke_embedding_dict["data_spec"] = sheet_name_list[sel_sheet_index]
    spoke_embedding_dict["total_significant_compounds"] = GLM_significant_compounds_mapped_to_SPOKE.shape[0]
    spoke_embedding_dict["total_significant_compounds_spoke_entry"] = entry_point_count
    spoke_embedding_dict["embedding"] = spoke_vector
    return spoke_embedding_dict


def get_significant_compounds_with_disease_association(compound_type, sample, sel_sheet_index, data_path, pvalue_thresh=0.05):
    if compound_type == "targeted":
        model_excel_file = os.path.join(data_path, 'GLM_result_targeted_compounds_{}_sample.xlsx'.format(sample))
        mapping_file = os.path.join(data_path, "short_chain_fatty_acid_spoke_map.csv")
    else:
        model_excel_file = os.path.join(data_path, 'GLM_result_global_compounds_{}_sample.xlsx'.format(sample))
        mapping_file = os.path.join(data_path, "global_metabolomics_compound_spoke_map.csv")

    GLM_out = pd.read_excel(model_excel_file, sheet_name=sheet_name_list[sel_sheet_index], engine='openpyxl')
    compound_spoke_map = pd.read_csv(mapping_file)
    GLM_out.dropna(subset=["model_converged_flag"], inplace=True)
    GLM_out = GLM_out[GLM_out["model_converged_flag"] == 1]
    GLM_out_significant_compounds = GLM_out[GLM_out.pvalue < pvalue_thresh]
    GLM_significant_compounds_mapped_to_SPOKE = pd.merge(GLM_out_significant_compounds, compound_spoke_map, left_on="analyte_name", right_on="name")[["disease_coeff", "spoke_identifer"]]
    GLM_significant_compounds_mapped_to_SPOKE.drop_duplicates(inplace=True)
    return GLM_significant_compounds_mapped_to_SPOKE


if __name__ == "__main__":
    main()
