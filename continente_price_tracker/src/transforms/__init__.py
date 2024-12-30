"""
Given a product description, extract the following structured fields:
- weight: numeric value representing the weight (default to "none" if not present)
- weight_unit: the unit of the weight (e.g., g, kg, ml; default to "none" if not present)
- quantity: the number of items (default to 1 if not specified)
- qty_unit: the unit for quantity (default to "un" if not specified)

If the quantity is given as text (e.g., "uma duzia", "meia duzia"), convert it to a numeric value.

Example inputs and outputs:

1. "requeijão de ovelha auchan à mesa em portugal cultivamos o bom seia 200 g"
   - weight: 200, weight_unit: "g", quantity: 1, qty_unit: "un"
   
2. "açúcar polegar granulado 4x170ml"
   - weight: 170, weight_unit: "ml", quantity: 4, qty_unit: "un"
   
3. "açúcar polegar granulado 1 kg"
   - weight: 1, weight_unit: "kg", quantity: 1, qty_unit: "un"
   
4. "ovos class m uma duzia"
   - weight: none, weight_unit: "none", quantity: 1, qty_unit: "duzia"
   
5. "ovos class m meia duzia"
   - weight: none, weight_unit: "none", quantity: 0.5, qty_unit: "duzia"
   
6. "saquetas de cha 10 saquetas"
   - weight: none, weight_unit: "none", quantity: 10, qty_unit: "saquetas"
   
7. "rolos de papel 4 rolos"
   - weight: none, weight_unit: "none", quantity: 4, qty_unit: "rolos"

8. "iogurte natural 120(94)g"
   - weight: 120, weight_unit: "g", quantity: 1, qty_unit: "un"

9. "iogurte natural 4uni"
   - weight: none, weight_unit: "none", quantity: 4, qty_unit: "un"

10. "iogurte 90+7 oferta"
    - weight: none, weight_unit: "none", quantity: 90, qty_unit: "un"

11. "vassoura de madeira"
    - weight: none, weight_unit: "none", quantity: 1, qty_unit: "un"

12. "Vinho Tinto Tetra Brick Pingo Doce 1 L"
   - weight: 1, weight_unit: "l", quantity: 1, qty_unit: "un"

Now, extract the structured data for the following input:

Input: "{input_text}"

"""