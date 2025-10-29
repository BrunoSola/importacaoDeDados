// src/importers/nfe.js
const FIXED_PARENT_GUID = '82024372-8ca7-869a-ac13-e0a3ef95396f'; // ajuste conforme seu padrão
const CHILD_FORM_ID = 10741; // Id do relacionamento filho no Flowch

function buildNfePayload(master, itens){
  return {
    "__recordguid__": FIXED_PARENT_GUID,
    "__relationships__": [
      {
        Id: CHILD_FORM_ID,
        childrens: [],
        records: itens.map(it => ({
          "__record_parent_guid__": FIXED_PARENT_GUID,
          xProd: it.xProd
          // mapeie os demais campos do item aqui
        }))
      }
    ],
    // campos do pai
    nfe_chave: master.nfe_chave
    // ...outros campos do pai
  };
}

module.exports = { buildNfePayload };
