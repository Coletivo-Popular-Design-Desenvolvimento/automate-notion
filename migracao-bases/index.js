import { Client } from "@notionhq/client"
import { config } from "dotenv"
import fetch from 'node-fetch';
import fs from 'fs';
import { type } from "os";


config()

const notion = new Client({ auth: process.env.NOTION_API_KEY })

class NotionMigrator {
    constructor() {
        this.sourceDbId = null
        this.targetDbId = null
        this.pageLookup = {}
        this.parentLookUp = {}
        this.subItemLookUp = {}
        this.referenceColumns = []
    }

    mapFilter(filterType, filterValue) {
        
        switch(filterType){
            case "rollup":
                return {
                    "any": {
                        "rich_text": {
                            "contains": filterValue
                        }
                    }};
            case "select":
                return {
                    "equals": filterValue
                    };
            default: 
                return {
                "contains": filterValue
            }
        }
    }

    async processAndUploadMedia(url, fileName, contentType) {

        
        // 1. Criar o objeto de upload de arquivo
        const createResponse = await notion.fileUploads.create({
            filename:fileName, 
            content_type: contentType, 
            mode:"single_part"
        });

        const fileUploadId = createResponse.id;

        // 2. Enviar o conteúdo do arquivo
        // (A implementação exata varia muito dependendo da biblioteca HTTP que você usa,
        // pois 'notionClient.request' geralmente não lida com 'multipart/form-data' facilmente.)
        // 1. Download do arquivo
        const response = await fetch(url);
        if (!response.ok) {
            console.error(`Falha ao baixar o arquivo: ${url}`);
            return null; // Ou jogue um erro, dependendo da sua estratégia de erro
        }
        const fileBuffer = await response.arrayBuffer();

        const fileBlob = new Blob([fileBuffer], {type:contentType});
        const sendResponse = await notion.fileUploads.send({
            file_upload_id: fileUploadId,
            file: {
                data: fileBlob,
                filename: fileName
            }
        });

        // 3. Retornar a estrutura do objeto para anexar o arquivo
        if (sendResponse.status === 'uploaded') {
            return {
                type: 'file_upload',
                file_upload: {
                    id: fileUploadId,
                },
            };
        } else {
            throw new Error(`Upload falhou com status: ${sendResponse.data.status}`);
        }
                
    }


    async getNewMediaObject(mediaObject, fallbackFileName = 'arquivo') {
        if (mediaObject?.type === 'file') {
            const url = mediaObject.file.url;
            // Tenta inferir o nome e tipo do arquivo da URL (melhorar isso para produção)
            const urlParts = url.split('/');
            const fileNameWithParams = urlParts[urlParts.length - 1].split('?')[0];
            
            // **Este contentType e fileName devem ser precisos para o upload funcionar**
            const contentType = 'image/png'; // Exemplo: Tente adivinhar ou use um default
            
            return await this.processAndUploadMedia(url, fileNameWithParams || fallbackFileName, contentType);
        } 
        
        // Se for 'external' ou 'emoji' ou qualquer outro tipo, apenas retorna a estrutura original
        return mediaObject;
    }

    async getSourceContent(sourcePage) {
        
        const childrenBlocks = await notion.blocks.children.list({
            block_id:sourcePage.id
        });
        const mappedItems = await Promise.all(childrenBlocks.results.map(async item=>this.getChildBlock(item)));
        return mappedItems;
    }

    async getChildBlock(child_request){
        const { id:child_id, object, parent, created_time, last_edited_time, ...child} = child_request;
        let result = {}
        let mediaTypes = ["file", "image", "pdf", "video", "audio"]

        if(mediaTypes.indexOf(child.type) >= 0){
            const file = await this.getNewMediaObject(child[child.type])
            result = {...result, [child.type]:file}
        }else if(child.type == "paragraph"){
            result = {
                ...child, 
                paragraph: {
                    ...child.paragraph, 
                    rich_text: [
                        ...(child.paragraph.rich_text??[]).map( richText => {
                            if (richText.type === 'mention' && richText.mention && richText.mention.type === 'link_preview') {
                                const url = richText.mention.link_preview.url;
                                // Converte para o tipo 'text' com a formatação de link
                                return {
                                    type: 'text',
                                    text: {
                                        content: url,
                                        link: { url: url }
                                    },
                                    annotations: richText.annotations, // Mantém a formatação original
                                    plain_text: url,
                                    href: url
                                };
                            }
                            return richText;
                        })]
                    }
                }
        } else if(child.type === "link_preview"){
            const url = child.link_preview.url;
            result = {
                type:"paragraph",
                paragraph: {
                    rich_text: [{
                        type: 'text',
                        text: {
                            content: url,
                            link: { url: url }
                        },
                        plain_text: url,
                        href: url                
                    }]
                }
            }
        } else {
            result = child;
        }
        if(child.has_children){
            const childrenBlocks = await notion.blocks.children.list({
                block_id:child_id
            });
            const mappedItems = await Promise.all(childrenBlocks.results.map(async item=>this.getChildBlock(item)));
            result = {...result, [child.type]:{...child[child.type], children:[...mappedItems]} };
        }
        return result;
    }

    // Busca todas as páginas do database de origem
    async getSourceData(databaseId, filters=[], includeDebugItems=false) {
        console.log(`📥 Buscando dados do database origem: ${databaseId}`)
        
        let allResults = []
        let hasMore = true
        let nextCursor = undefined

        while (hasMore) {
            let query = {
                data_source_id: databaseId,
                start_cursor: nextCursor,
                page_size: 100, 
                filter: filters && filters.length > 0 ?  {
                    and: filters.map(item => ({
                        property: item.propertyName,
                        [item.propertyType] : this.mapFilter(item.propertyType, item.propertyValue)
                    }))
                } : undefined
            };
            if(includeDebugItems){
                query = {...query, filter: {...query.filter, and: [...query.filter.and,
                    {
                        or: [ 
                            {
                                property: "Nome",
                                "rich_text": {
                                    "contains": "[Dados] Tradução das demandas."
                                }
                            },
                            {
                                property: "Nome",
                                "rich_text": {
                                    "contains": "Criar script para Popular o banco de dados."
                                }
                            }
                        ]
                    }
                ]}}; 

            }

            const response = await notion.dataSources.query(query);

            allResults = [...allResults, ...response.results]
            hasMore = response.has_more
            nextCursor = response.next_cursor

            console.log(`   Carregadas ${allResults.length} páginas...`)
        }

        console.log(`✅ Total de páginas encontradas: ${allResults.length}`)
        return allResults
    }

    // Busca as propriedades do database de destino
    async getTargetSchema(databaseId) {
        console.log(`🔍 Analisando schema do database destino: ${databaseId}`)
        
        const response = await notion.databases.retrieve({
            database_id: databaseId
        })

        const properties = response.properties
        console.log(`✅ Propriedades encontradas: ${Object.keys(properties).join(', ')}`)
        
        return properties
    }

    async addTargetLegacyId(databaseID) {
        const response = await notion.databases.update({
            database_id:databaseID,
            properties:{
                "IdLegado": {
                    "number":{}
                }
            }
        });
        console.log(`✅ Propriedade Id Legado criada`);
    }

    async cloneDatabase(sourceDatabase, targetPageId, newDatabaseTitle, simulate=true) {
        try {
            console.log('🔍 Obtendo informações da base de dados de origem...');

            console.log(`✅ Base de dados encontrada: ${sourceDatabase.title[0]?.plain_text || 'Sem título'}`);
            console.log(`📊 Propriedades encontradas: ${Object.keys(sourceDatabase.properties).length}`);
            console.log('🚀 Criando nova base de dados...');

            // 3. Criar a nova base de dados com as propriedades copiadas
            const databaseProps = {
                    parent: {
                        type: 'page_id',
                        page_id: targetPageId
                    },
                    title: [
                        {
                            type: 'text',
                            text: {
                                content: newDatabaseTitle
                            }
                        }
                    ]
                };

            if(!simulate){
                const newDatabase = await notion.databases.create(databaseProps);
                const newDataSource = await notion.dataSources.retrieve({
                    data_source_id: newDatabase.data_sources[0].id 
                })
                console.log(`✅ Nova base de dados criada com sucesso!`);
                console.log(`🆔 ID da nova base de dados: ${newDatabase.id}`);
                console.log(`🔗 URL: ${newDatabase.url}`);
                return {...newDataSource};
            } else {
                console.log(`✅ Simulañçao de criação de bases realizada com sucesso: Propriedades:`, databaseProps);
                return {...databaseProps, id:"Simulação"};
            }

        } catch (error) {
            console.error('❌ Erro ao clonar base de dados:', error.message);
            
            if (error.code === 'object_not_found') {
                console.error('💡 Verifique se o ID da base de dados ou página está correto e se a integração tem acesso.');
            } else if (error.code === 'unauthorized') {
                console.error('💡 Verifique se o token de autenticação está correto e se a integração tem as permissões necessárias.');
            }
            
            throw error;
        }
    }

    async updateSchema(targeDbId, newProperties, simulate=true) {
        try {

            if(!simulate){
                const newData = await notion.dataSources.update(
                {
                    data_source_id: targeDbId,
                    properties: { ... newProperties}
                }
                )
                return {...newData};
            } else {
                return {...newProperties};
            }

        } catch (error) {
            console.error('❌ Erro ao clonar base de dados:', error.message);
            
            if (error.code === 'object_not_found') {
                console.error('💡 Verifique se o ID da base de dados ou página está correto e se a integração tem acesso.');
            } else if (error.code === 'unauthorized') {
                console.error('💡 Verifique se o token de autenticação está correto e se a integração tem as permissões necessárias.');
            }
            
            throw error;
        }
    }

    mapPropertyStructure(propertyConfig, propertyName, targetSchema) {

        const cleanProperty = {
            type: propertyName == "IdLegado" ? "number" : propertyConfig.type
        }
        let propName = propertyName;

        // Copiar configurações específicas de cada tipo de propriedade
        switch (cleanProperty.type) {
            case 'title':
                var objKeys = Object.keys(targetSchema.properties);
                var titlePropertyName = objKeys.filter(item => targetSchema.properties[item].type=="title");
                cleanProperty.title = { }
                cleanProperty.name = propertyName;
                propName = titlePropertyName;
                break
            case 'rich_text':
                cleanProperty.rich_text = {}
                break
            case 'number':
                cleanProperty.number = propertyConfig?.number || {}
                break
            case 'select':
                cleanProperty.select = {
                    options: propertyConfig.select.options || []
                }
                break
            case 'multi_select':
                cleanProperty.multi_select = {
                    options: propertyConfig.multi_select.options || []
                }
                break
            case 'date':
                cleanProperty.date = {}
                break
            case 'people':
                cleanProperty.people = {}
                break
            case 'files':
                cleanProperty.files = {}
                break
            case 'checkbox':
                cleanProperty.checkbox = {}
                break
            case 'url':
                cleanProperty.url = {}
                break
            case 'email':
                cleanProperty.email = {}
                break
            case 'phone_number':
                cleanProperty.phone_number = {}
                break
            case 'formula':
                cleanProperty.formula = propertyConfig.formula || {}
                break
            case 'relation':
                cleanProperty.relation = propertyConfig.relation
                break
            case 'rollup':
                cleanProperty.rollup = propertyConfig.rollup || {}
                break
            case 'created_time':
                cleanProperty.created_time = {}
                break
            case 'created_by':
                cleanProperty.created_by = {}
                break
            case 'last_edited_time':
                cleanProperty.last_edited_time = {}
                break
            case 'last_edited_by':
                cleanProperty.last_edited_by = {}
                break
            case 'status':
                cleanProperty.type = "select"
                cleanProperty.name = propertyConfig.name,
                    cleanProperty.select = {
                        options: propertyConfig?.status?.options?.map(item => {
                            return {
                                id: item.id,
                                name: item.name,
                                color: item.color
                            }
                        }) || []
                    }
                break
            case 'unique_id':
                {
                    cleanProperty.unique_id = {
                        prefix: propertyConfig.unique_id.prefix || null
                    }
                    if (propertyName == "ID") {
                        cleanProperty["IdLegado"] = {
                            number: {}
                        }
                    }
                }
                break; 
            default:
                // Para tipos não mapeados, copiar a configuração inteira (exceto o ID)
                const { id, ...configWithoutId } = propertyConfig
                cleanProperty[propertyConfig.type] = configWithoutId[propertyConfig.type] || {}
        }

        return {property: cleanProperty, name: propName}
    }

    mapPropertyValue(sourceSchema, sourcePage, propName) {
        const targetProp = sourceSchema[propName]
        const sourceProp = sourcePage.properties[propName]
        let outputValue = null;
        let outputPropertyName = propName;

        // Mapeia diferentes tipos de propriedade
        switch (targetProp.type) {
            case 'title':
                if (sourceProp.title) {
                    outputValue = {
                        title: sourceProp.title
                    }
                }
                break

            case 'rich_text':
                if (sourceProp.rich_text) {
                    outputValue = {
                        rich_text: sourceProp.rich_text
                    }
                }
                break

            case 'number':
                if (sourceProp.number !== null && sourceProp.number !== undefined) {
                    outputValue = {
                        number: sourceProp.number
                    }
                }
                break

            case 'select':
                if (sourceProp.select) {
                    outputValue = {
                        select: {
                            name: sourceProp.select.name
                        }
                    }
                }
                break
            case 'status':
                    if(sourceProp.status){
                        outputValue = {
                            select: {
                                name: sourceProp.status.name
                            }
                        }                
                    }
                break;
            case 'date': {
                if(sourceProp.date){
                    outputValue = {
                        date: sourceProp.date
                    }
                }
                break;
            }             
            case 'multi_select':
                if (sourceProp.multi_select) {
                    outputValue = {
                        multi_select: sourceProp.multi_select.map(item => {
                            const { id, ...multi_select } = item
                            return multi_select
                        })
                    }
                }
                break

            case 'date':
                if (sourceProp.date) {
                    outputValue = {
                        date: sourceProp.date
                    }
                }
                break

            case 'checkbox':
                outputValue = {
                    checkbox: sourceProp.checkbox || false
                }
                break

            case 'url':
                if (sourceProp.url) {
                    outputValue = {
                        url: sourceProp.url
                    }
                }
                break

            case 'email':
                if (sourceProp.email) {
                    outputValue = {
                        email: sourceProp.email
                    }
                }
                break

            case 'phone_number':
                if (sourceProp.phone_number) {
                    outputValue = {
                        phone_number: sourceProp.phone_number
                    }
                }
                break

            case 'relation':
                if (sourceProp.relation && sourceProp.relation.length > 0) {
                    const isSelfReference = propName.indexOf("Parent-item") >= 0 || propName.indexOf("Sub-item") >=0;
                    if(isSelfReference && propName === "Parent-item")
                    {
                        this.parentLookUp = {...this.parentLookUp, [sourcePage.id]:{
                            relation: sourceProp.relation.map(item=> {
                                return {id:item.id}
                            })}};

                    }else if(isSelfReference && propName === "Sub-item")
                    {
                        this.subItemLookUp = {...this.subItemLookUp, [sourcePage.id]:{
                            relation: sourceProp.relation.map(item=> {
                                return {id:item.id}
                            })}};
                    } else if( !isSelfReference ) {
                        outputValue = { relation: sourceProp.relation }
                    }
                    //Ignore automatic self references.
                }
                break

            case 'people':
                if (sourceProp.people && sourceProp.people.length > 0) {
                    outputValue = {
                        people: sourceProp.people.map(item => {
                            return {
                                id: item.id
                            }
                        })
                    }
                }
                break
            case 'unique_id':
                outputValue = {
                    number: sourcePage.properties.ID.unique_id.number
                }     
                outputPropertyName = "IdLegado";           
                break
            default:
                console.log(`⚠️  Tipo de propriedade não suportado: ${targetProp.type} (${propName})`)
            
        }
        return {propertyName:outputPropertyName, propertyValue:outputValue};
    }

    // Converte as propriedades de uma página para o formato correto
    mapPropertyValues(sourcePage, sourceSchema, sourceId) {
        const mappedProperties = {}

        Object.keys(sourceSchema).forEach(propName => {
            const {propertyName, propertyValue} = this.mapPropertyValue(sourceSchema, sourcePage, propName);
            if(propertyValue && propertyValue != null){
                mappedProperties[propertyName] = propertyValue;
            }
        })

        return mappedProperties
    }


    // Cria uma nova página no database de destino
    async fixRelations(targetDbId) {

        const structure = {
            data_source_id: targetDbId,
            properties:{
                "Parent-item": {
                    relation: {
                        data_source_id: targetDbId,
                        dual_property:{
                            "synced_property_name": "Sub-item"
                        }
                    }
                },
                "Sub-item": {
                    relation: {
                        data_source_id: targetDbId,
                        single_property:{}
                    }
                }
            }
        }
        await notion.dataSources.update(structure);


        const parentKeys = Object.keys(this.parentLookUp);
        await Promise.all(parentKeys.map(async key => {
            const destinationId = this.pageLookup[key];
            const pageUpdate = {
                page_id:destinationId,
                properties: {
                    "Parent-item": {
                        relation: this.parentLookUp[key].relation.map(item=>{
                            return {
                                id: this.pageLookup[item.id]
                            };
                        }).filter(item=>item.id && item.id != null)
                    }
                }
            };
            await notion.pages.update(pageUpdate);
        }));

        const subKeys = Object.keys(this.subItemLookUp);
        await Promise.all(subKeys.map(async key => {
            const destinationId = this.pageLookup[key];
            const pageUpdate = {
                page_id:destinationId,
                properties: {
                    "Sub-item": {
                        relation: this.subItemLookUp[key].relation.map(item=>{
                            return {
                                id: this.pageLookup[item.id]
                            };
                        }).filter(item=>item.id && item.id != null)
                    }
                }
            };
            await notion.pages.update(pageUpdate);
        }));

    }
    
    // Cria uma nova página no database de destino
    async createPage(targetDbId, properties, icon, cover, content, originalContent) {

        let req = {
            parent: {
                data_source_id: targetDbId
            },
            properties: properties
        };        

        if(icon) {
            const newIcon = await this.getNewMediaObject(icon, 'page_icon');
            req = {...req, icon: newIcon }
        }
        if(cover) {
            const newCover = await this.getNewMediaObject(cover, 'page_cover');
            req = {...req, cover: newCover }
        }

        try {
            const response = await notion.pages.create(req);
            if(content){
                await this.appendChildren(response.id, content);
            }
            await this.appendComments(response.id, originalContent.id)
            this.pageLookup = {...this.pageLookup,[originalContent.id]: response.id};
            return response
        } catch (error) {
            console.error('❌ Erro ao criar página:', error.message)
            throw error
        }
    }

    async appendComments(pageId, originalId){
        const comments = await notion.comments.list({
            block_id: originalId
        });

        for(var index in comments.results) {
            const item = comments.results[parseInt(index)];
            const itemBlock = await this.getChildBlock(item);
            const{ discussion_id, parent, ...itemCleanup} = itemBlock;
            const req = {...itemCleanup, parent:{page_id:pageId} }
            try{
                await notion.comments.create(req);
            }catch(error){
                console.log(error);
                throw(error);
            }
        }
    }

    async appendChildren(pageId, children){
        for(var index in children) {
            const item = children[parseInt(index)];
            const {children:childrensOfItem, has_children,archived, in_trash, last_edited_by, created_by, ...req} = item;
            let childrensOfBulletedList = [];
            if(item.bulleted_list_item && item.has_children){
                childrensOfBulletedList = [...req.bulleted_list_item.children];
                req.bulleted_list_item.children = [];
            }
            try{
                
                const result = await notion.blocks.children.append({
                    block_id: pageId,
                    children: [req]
                });

                if(childrensOfItem && has_children){
                    await this.appendChildren(result.results[0].id, childrensOfItem);
                }

                if(item.bulleted_list_item && has_children){
                    await this.appendChildren(result.results[0].id, childrensOfBulletedList);
                }

            } catch(error){
                console.log(error)
                throw error;
            }

        }
    }


    // Executa a migração completa
    async migrate(sourceDbId, targetPageId, targetDBName, options = {}) {
        const { dryRun = false, batchSize = 10 } = options

        console.log('🚀 Iniciando migração...')
        console.log(`   Database origem: ${sourceDbId}`)
        console.log(`   Database destino: ${this.targetDBName}`)
        console.log(`   Modo dry-run: ${dryRun ? 'SIM' : 'NÃO'}`)
        console.log('─'.repeat(50))

        
        // try {

            const project = await this.getSourceData("e4919bf1-3b4b-4c11-ab60-1066b8446c37",[{
                propertyName:"Nome",
                propertyType:"rich_text",
                propertyValue: options.filters[0]
            }])


            const filters = this.mapDefaultFilters(project, options);


            // 1. Buscar dados da origem
            const sourceData = await this.getSourceData(sourceDbId, filters, false)

            // 1. Obter a estrutura da base de dados de origem
            const sourceDatabase = await notion.dataSources.retrieve({
                data_source_id: sourceDbId
            });

            let targetSchema = await this.cloneDatabase(sourceDatabase, targetPageId, targetDBName, options.dryRun);
            
            const targetDbId = targetSchema.id;

            // 3. Migrar dados
            console.log('📤 Iniciando transferência de dados...')
            
            let successCount = 0
            let errorCount = 0

            for (let i = 0; i < sourceData.length; i += batchSize) {
                const batch = sourceData.slice(i, i + batchSize)
                
                console.log(`   Processando lote ${Math.floor(i/batchSize) + 1}/${Math.ceil(sourceData.length/batchSize)}...`)

                for (const page of batch) {
                    // try {

                        const mappedProperties = this.mapPropertyValues(page, page.properties, sourceDbId)
                    
                        let newProperties = {}
                        Object.keys(mappedProperties).forEach( propName => {
                            if(!targetSchema?.properties || !targetSchema?.properties[propName]){
                                const {property, name} = this.mapPropertyStructure(sourceDatabase.properties[propName], propName, targetSchema);
                                newProperties[name] = property
                            }
                        })
                        if(newProperties != {}){
                            targetSchema = await this.updateSchema(targetDbId, newProperties, options.dryRun)
                        }

                        const content = await this.getSourceContent(page);


                        if (dryRun) {
                            console.log(`   [DRY-RUN] Simularia criação de página com propriedades:`, Object.keys(mappedProperties))
                        } else {
                            await this.createPage(targetDbId, mappedProperties, page.icon, page.cover, content, page);
                            successCount++
                        }

                        // Delay para evitar rate limiting
                        await new Promise(resolve => setTimeout(resolve, 100))

                    // } catch (error) {
                    //     console.error(`❌ Erro ao migrar página ${page.id}:`, error.message)
                    //     errorCount++
                    // }
                }
            }

            await this.fixRelations(targetDbId);

            console.log('─'.repeat(50))
            console.log('✅ Migração concluída!')
            console.log(`   Páginas migradas com sucesso: ${successCount}`)
            if (errorCount > 0) {
                console.log(`   Páginas com erro: ${errorCount}`)
            }


        // } catch (error) {
        //     console.error('❌ Erro na migração:', error.message)
        //     throw error
        // }
    }

    mapDefaultFilters(project, options) {
        return [{
            propertyName: "Projeto",
            propertyType: "relation",
            propertyValue: project[0].id
        },
        {
            propertyName: "Tipo da Tarefa",
            propertyType: "select",
            propertyValue: options.filters[1]
        }
        ]
    }
}

// Exemplo de uso
async function main(sourceDB, targetPageId, newDatabaseTitle, simulate=true, filters) {
    const migrator = new NotionMigrator()
    
    try {
        await migrator.migrate(sourceDB, targetPageId, newDatabaseTitle, { 
            dryRun: simulate,
            filters: filters,
            batchSize: 10
        })

    } catch (error) {
        console.error('💥 Falha na migração:', error)
    }
}

// Para usar como módulo
export default NotionMigrator

if(process.argv.length >= 5) {
    main(process.argv[2], process.argv[3], process.argv[4], process.argv[5]!=="false", process.argv.slice(6))
} else {
    console.log("Utilização: \nnode start source_database_id destination_page_id destination_database_name [true(default)|false]\n - Simulação [true - Default, false - execução]), propertyName=propertyValue... filtros para a pesquisa de dados");
}


