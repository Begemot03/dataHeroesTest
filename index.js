"use strict";

const pg = require('pg');
const fs = require('fs');

const config = {
    connectionString: "postgres://candidate:62I8anq3cFq5GYh2u4Lh@rc1b-r21uoagjy1t7k77h.mdb.yandexcloud.net:6432/db1",
    ssl: {
        rejectUnauthorized: true,
        ca: fs.readFileSync('/home/andrew/.postgresql/root.crt').toString(),
    },
};

const client = new pg.Client(config);

client.connect((err) => {
    if(err) throw err;
});



async function createRickAndMortyTable(tableName) {
    await client.query(`drop table if exists ${tableName}`);

    const query = `create table if not exists ${tableName} ( 
        id SERIAL PRIMARY KEY,
        name TEXT,
        data JSONB
    )`;

    await client.query(query);

    console.log(`table [ ${tableName} ] is created`);
}

function transformCharacterData(character) {
    const newFormData = {
        id: character.id,
        name: character.name,
        data: {
            status: character.status,
            species: character.species,
            type: character.type,
            gender: character.gender,
            origin: character.origin.name,
            location: character.location.name,
            image: character.image,
            episode: character.episode,
            last_modified: character.modified,
            url:  character.url,
            created: character.created,
        },
    };

    return newFormData;
}

async function getCharactersInfo(url) {
    const result = [];

    try {
        let response = await fetch(url);
        let json = await response.json();
        result.push(...json.results.map(transformCharacterData));

        while(json.info.next !== null) {
            response = await fetch(json.info.next);
            json = await response.json();
            result.push(...json.results.map(transformCharacterData));
        }
        
        console.log("Данные о персонажах получены");

        return {
            ok: true, 
            characters: result,
        };
    } catch(err) {
        console.log(`Ошибка при получении данных\nerr: ${err}`);
        return {
            ok: false
        };
    }
}

async function insertCharacters(tableName, characters) {
    try {
        const query = `insert into ${tableName} (name, data) values ($1, $2)`;

        for(const character of characters) {
            const res = await client.query(query, [character.name, character.data]);

            console.log(`Данные о персонаже [ ${character.name} ] добавлены в таблицу [ ${tableName} ]`);
        }

        return { ok: true, };
    } catch(err) {
        console.log(`Ошибка при добавлении информации о персонажах\nerr: ${err}`)

        return { ok: false, };
    }
    
}


(async function () {
    const tableName = "Begemot03"
    const apiUrl = "https://rickandmortyapi.com/api/character";
    
    await createRickAndMortyTable(tableName);

    let res_getInfo = await getCharactersInfo(apiUrl);

    if(res_getInfo.ok) {
        let res = await insertCharacters(tableName, res_getInfo.characters);
    }

    await client.end();
})();