import { Client } from "@notionhq/client"
import { config } from "dotenv"

config()

const notion = new Client({ auth: process.env.NOTION_API_KEY })

class NotionMigrator {
    constructor() {
        this.sourceDbId = null
        this.targetDbId = null
    }

    // Busca todas as páginas do database de origem
    async getSourceData(databaseId) {
        console.log(`📥 Buscando dados do database origem: ${databaseId}`)
        
        let allResults = []
        let hasMore = true
        let nextCursor = undefined

        while (hasMore) {
            const response = await notion.databases.query({
                database_id: databaseId,
                start_cursor: nextCursor,
                page_size: 100, 
                // filter: {
                //     //FILTROS AQUI - https://developers.notion.com/reference/post-database-query-filter
                // }
            })

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

    // Converte as propriedades de uma página para o formato correto
    mapProperties(sourcePage, targetSchema) {
        const mappedProperties = {}

        Object.keys(targetSchema).forEach(propName => {
            const targetProp = targetSchema[propName]
            const sourceProp = sourcePage.properties[propName]

            if (!sourceProp) {
                console.log(`⚠️  Propriedade "${propName}" não encontrada na página origem`)
                return
            }

            // Mapeia diferentes tipos de propriedade
            switch (targetProp.type) {
                case 'title':
                    if (sourceProp.title) {
                        mappedProperties[propName] = {
                            title: sourceProp.title
                        }
                    }
                    break

                case 'rich_text':
                    if (sourceProp.rich_text) {
                        mappedProperties[propName] = {
                            rich_text: sourceProp.rich_text
                        }
                    }
                    break

                case 'number':
                    if (sourceProp.number !== null && sourceProp.number !== undefined) {
                        mappedProperties[propName] = {
                            number: sourceProp.number
                        }
                    }
                    break

                case 'select':
                    if (sourceProp.select) {
                        mappedProperties[propName] = {
                            select: sourceProp.select
                        }
                    }
                    break

                case 'multi_select':
                    if (sourceProp.multi_select) {
                        mappedProperties[propName] = {
                            multi_select: sourceProp.multi_select
                        }
                    }
                    break

                case 'date':
                    if (sourceProp.date) {
                        mappedProperties[propName] = {
                            date: sourceProp.date
                        }
                    }
                    break

                case 'checkbox':
                    mappedProperties[propName] = {
                        checkbox: sourceProp.checkbox || false
                    }
                    break

                case 'url':
                    if (sourceProp.url) {
                        mappedProperties[propName] = {
                            url: sourceProp.url
                        }
                    }
                    break

                case 'email':
                    if (sourceProp.email) {
                        mappedProperties[propName] = {
                            email: sourceProp.email
                        }
                    }
                    break

                case 'phone_number':
                    if (sourceProp.phone_number) {
                        mappedProperties[propName] = {
                            phone_number: sourceProp.phone_number
                        }
                    }
                    break

                case 'relation':
                    if (sourceProp.relation) {
                        mappedProperties[propName] = {
                            relation: sourceProp.relation
                        }
                    }
                    break

                default:
                    console.log(`⚠️  Tipo de propriedade não suportado: ${targetProp.type} (${propName})`)
            }
        })

        return mappedProperties
    }

    // Cria uma nova página no database de destino
    async createPage(targetDbId, properties) {
        try {
            const response = await notion.pages.create({
                parent: {
                    database_id: targetDbId
                },
                properties: properties
            })
            return response
        } catch (error) {
            console.error('❌ Erro ao criar página:', error.message)
            throw error
        }
    }

    // Executa a migração completa
    async migrate(sourceDbId, targetDbId, options = {}) {
        const { dryRun = false, batchSize = 10 } = options

        console.log('🚀 Iniciando migração...')
        console.log(`   Database origem: ${sourceDbId}`)
        console.log(`   Database destino: ${targetDbId}`)
        console.log(`   Modo dry-run: ${dryRun ? 'SIM' : 'NÃO'}`)
        console.log('─'.repeat(50))

        try {
            // 1. Buscar dados da origem
            const sourceData = await this.getSourceData(sourceDbId)

            // 2. Buscar schema do destino
            const targetSchema = await this.getTargetSchema(targetDbId)

            // 3. Migrar dados
            console.log('📤 Iniciando transferência de dados...')
            
            let successCount = 0
            let errorCount = 0

            for (let i = 0; i < sourceData.length; i += batchSize) {
                const batch = sourceData.slice(i, i + batchSize)
                
                console.log(`   Processando lote ${Math.floor(i/batchSize) + 1}/${Math.ceil(sourceData.length/batchSize)}...`)

                for (const page of batch) {
                    try {
                        const mappedProperties = this.mapProperties(page, targetSchema)
                        
                        if (dryRun) {
                            console.log(`   [DRY-RUN] Simularia criação de página com propriedades:`, Object.keys(mappedProperties))
                        } else {
                            await this.createPage(targetDbId, mappedProperties)
                            successCount++
                        }

                        // Delay para evitar rate limiting
                        await new Promise(resolve => setTimeout(resolve, 100))

                    } catch (error) {
                        console.error(`❌ Erro ao migrar página ${page.id}:`, error.message)
                        errorCount++
                    }
                }
            }

            console.log('─'.repeat(50))
            console.log('✅ Migração concluída!')
            console.log(`   Páginas migradas com sucesso: ${successCount}`)
            if (errorCount > 0) {
                console.log(`   Páginas com erro: ${errorCount}`)
            }

        } catch (error) {
            console.error('❌ Erro na migração:', error.message)
            throw error
        }
    }
}

// Exemplo de uso
async function main(sourceDB, destinationDB, simulate=true) {
    const migrator = new NotionMigrator()
    try {
        await migrator.migrate(sourceDB, destinationDB, { 
            dryRun: !simulate, 
            batchSize: 10 
        })

    } catch (error) {
        console.error('💥 Falha na migração:', error)
    }
}

// Para usar como módulo
export default NotionMigrator

if(process.argv.length >= 4) {
    main(process.argv[2], process.argv[3],process.argv[4]==="true")
} else {
    console.log("Utilização: \nnode start source_database_id destination_database_id [true|false(default)]\n - true no final confirmará a execução, false apenas realizará uma simulação");
}


