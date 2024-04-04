# Tercera_Entrega-Bautista_Arganaraz_Londero

## Trabajo con el archivo 'docker-compose.yaml':

- Para correr el Docker Compose ejecutar el siguiente comando:

```
docker-compose up
```

- Luego ir al navegador y colocar la siguiente dirección:

```
http://localhost:8080/
```

- Ahi se puede ver el DAG principal, desde donde se puede ejecutar manualmente y ver el Graph donde aparece el estado de las funciones ejecutadas.

## Trabajo con el archivo 'Dockerfile':

- Para construir la imagen a partir del DockerFile ejecutar el siguiente comando:

```
docker build -t tercera_entrega .
```

- Para correr el contenedor a partir de la imagen generada ejecutar el siguiente comando:

```
docker run tercera_entrega
```

## Base de datos:

- Con DBeaver se puede ver la tabla creada con los datos al conectarse a la base de datos utilizando las credenciales presentes en el archivo 'variables.json'.