import graphviz
import textwrap

# Function to wrap text for node labels
def wrap_label(text, width=35):
    return '\n'.join(textwrap.wrap(text, width=width))

# Create a new directed graph
dot = graphviz.Digraph(comment='GRUPO_DAMM_JOIN0059 IS_VALIDATED_DESC Decision Tree')
dot.attr(rankdir='TB', size='15,15', fontsize='10') # Top-to-Bottom layout, adjust size

# Define node styles
dot.attr('node', shape='box', style='rounded,filled', fillcolor='lightblue', fontsize='9')
decision_opts = {'shape': 'diamond', 'fillcolor': 'lightyellow'}
error_opts = {'shape': 'box', 'style': 'filled', 'fillcolor': 'lightcoral'}
success_opts = {'shape': 'box', 'style': 'filled', 'fillcolor': 'lightgreen'}
check_opts = {'shape': 'box', 'style': 'filled', 'fillcolor': 'whitesmoke'} # Intermediate checks

# --- Nodes ---

# Start
dot.node('start', 'START', shape='Mdiamond', fillcolor='gray', style='filled', fontcolor='white')

# Q1: E80 Line Check
dot.node('q1_e80_line', wrap_label("Is ARBPL Prefix in ('14','17','53','54','11','12','15','16')?"), **decision_opts)

# Q1a: E80 SKU Check
dot.node('q1a_e80_sku', wrap_label("Is MATNR NOT IN (LGV.SKU UNION SDM.SKU)?"), **decision_opts)
dot.node('msg1_e80_exclusion', wrap_label("Msg 1: E80 Exclusion - SKU not found in E80 Views for relevant line. (Details based on MMSTA, GROUP, BOM, PAL, etc.)"), **error_opts)

# Q2: MES BOM Check
dot.node('q2_mes_bom', wrap_label("Is MATNR NOT found as Parent in MESDB bom_item?"), **decision_opts)
dot.node('msg2_no_mes_bom', wrap_label("Msg 2: MES Check - Product has no Parent BOM entry in MESDB."), **error_opts)

# Q3: Product Family Check
dot.node('q3_has_family', wrap_label("Does Product have a Family (ZZFAMILIA <> '')?"), **decision_opts)
dot.node('msg3_no_family', wrap_label("Msg 3: Product Check - No ZZFAMILIA assigned in Product Master."), **error_opts)

# Checks within Q3=YES path
dot.node('q3a_family_speed', wrap_label("Is Family NOT in MST_FAMILY_SPEED?"), **decision_opts)
dot.node('msg4_no_family_speed', wrap_label("Msg 4: Config Check - Family has no Speed defined."), **error_opts)

dot.node('q3b_mes_route', wrap_label("Is NO Route defined in item_process_link for MATNR/PROD_UNIT?"), **decision_opts)
dot.node('msg5_no_mes_route', wrap_label("Msg 5: MES Check - No Process Route defined in MESDB."), **error_opts)

dot.node('q3c_multiple_routes', wrap_label("Is PROD_UNIT NOT IN ('PRT...17','ELE...32') AND >1 Route exists in item_process_link?"), **decision_opts)
dot.node('msg6_multiple_routes', wrap_label("Msg 6: MES Check - Multiple routes found for non-exception Production Unit."), **error_opts)

dot.node('q3d_oper_item_spec', wrap_label("Is Family/ARBPL combination NOT in oper_item_spec?"), **decision_opts)
dot.node('msg7_no_oper_item_spec', wrap_label("Msg 7: MES Check - Family not configured for this Work Center in MES."), **error_opts)

dot.node('q3e_maq_ruta_status', wrap_label("Is ARBPL/Family combination NOT Active in FAMILIA_MAQUINA_RUTA_STATUS?"), **decision_opts)
dot.node('msg8_no_maq_ruta_status', wrap_label("Msg 8: Config Check - Family/Work Center combo not active/configured in MFSYSDB."), **error_opts)

dot.node('q3f_lgv_invalid', wrap_label("Is ARBPL Prefix in ('14','17','53','54') AND SKU exists in LGV view with IS_VALIDATED = 0?"), **decision_opts)
dot.node('msg9_lgv_invalid_desc', wrap_label("Msg 9: Validation Fail - Reason from LGV.IS_VALIDATED_DESC"), **error_opts)

dot.node('q3g_final_lgv_check', wrap_label("Final Check: SKU Status in LGV View (MFSYS_SDM_ART_LB)"), **check_opts) # More of a final state determination
dot.node('res_validation_ok', wrap_label("Validation OK"), **success_opts)
dot.node('res_lgv_invalid_desc_final', wrap_label("Validation Fail - Reason from LGV.IS_VALIDATED_DESC (Fallback)"), **error_opts)
dot.node('res_lgv_sku_not_found', wrap_label("E80 LGV Check: SKU not found in MFSYS_SDM_ART_LB"), **error_opts)


# --- Edges (Logic Flow) ---
dot.edge('start', 'q1_e80_line')

# Q1 Path
dot.edge('q1_e80_line', 'q1a_e80_sku', label=' YES ')
dot.edge('q1_e80_line', 'q2_mes_bom', label=' NO ') # Skip E80 SKU check if line irrelevant

# Q1a Path
dot.edge('q1a_e80_sku', 'msg1_e80_exclusion', label=' YES ')
dot.edge('q1a_e80_sku', 'q2_mes_bom', label=' NO ') # If SKU is found, continue checks

# Q2 Path
dot.edge('q2_mes_bom', 'msg2_no_mes_bom', label=' YES ')
dot.edge('q2_mes_bom', 'q3_has_family', label=' NO ')

# Q3 Path
dot.edge('q3_has_family', 'q3a_family_speed', label=' YES ')
dot.edge('q3_has_family', 'msg3_no_family', label=' NO ')

# Q3=YES Sub-path
dot.edge('q3a_family_speed', 'msg4_no_family_speed', label=' YES ')
dot.edge('q3a_family_speed', 'q3b_mes_route', label=' NO ')

dot.edge('q3b_mes_route', 'msg5_no_mes_route', label=' YES ')
dot.edge('q3b_mes_route', 'q3c_multiple_routes', label=' NO ')

dot.edge('q3c_multiple_routes', 'msg6_multiple_routes', label=' YES ')
dot.edge('q3c_multiple_routes', 'q3d_oper_item_spec', label=' NO ')

dot.edge('q3d_oper_item_spec', 'msg7_no_oper_item_spec', label=' YES ')
dot.edge('q3d_oper_item_spec', 'q3e_maq_ruta_status', label=' NO ')

dot.edge('q3e_maq_ruta_status', 'msg8_no_maq_ruta_status', label=' YES ')
dot.edge('q3e_maq_ruta_status', 'q3f_lgv_invalid', label=' NO ')

dot.edge('q3f_lgv_invalid', 'msg9_lgv_invalid_desc', label=' YES ')
dot.edge('q3f_lgv_invalid', 'q3g_final_lgv_check', label=' NO ')

# Final LGV Check Outcomes (Simplified representation)
# Note: This part simplifies the ELSE ISNULL(...) logic from the SQL
dot.edge('q3g_final_lgv_check', 'res_validation_ok', label=' IS_VALIDATED = 1 ')
dot.edge('q3g_final_lgv_check', 'res_lgv_invalid_desc_final', label=' IS_VALIDATED = 0 ')
dot.edge('q3g_final_lgv_check', 'res_lgv_sku_not_found', label=' SKU Not Found ')


# --- Render Graph ---
# You can change the format to 'pdf', 'svg', etc.
# The 'view=True' argument tries to open the generated file automatically.
try:
    dot.render('grupo_damm_is_validated_desc_tree', view=True, format='png', cleanup=True)
    print("Decision tree diagram 'grupo_damm_is_validated_desc_tree.png' created successfully.")
except graphviz.backend.ExecutableNotFound:
    print("Graphviz executable not found. Please install Graphviz and ensure it's in your system's PATH.")
    print("Saving DOT source file: 'grupo_damm_is_validated_desc_tree.gv'")
    dot.save('grupo_damm_is_validated_desc_tree.gv')
    dot.save('is_validated_desc.png')
except Exception as e:
    print(f"An error occurred: {e}")
    print("Saving DOT source file: 'grupo_damm_is_validated_desc_tree.gv'")
    dot.save('grupo_damm_is_validated_desc_tree.gv')