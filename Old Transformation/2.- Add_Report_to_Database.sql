/*
Este Query sirve para limpiar la información del reporte incluyendo Marca, Subproducto; también agrega una columna de fecha para
poder identificar la información; al final muestra lo que se tiene que colocar en el Dashboard.

NOTA: Utilizar Cntl + F para cambiar el nombre del reporte en todas las lineas 

NOTA1: Se tiene que cambiar la fecha del día en la segunda linea

NOTA2: Los fines de semana hay que cambiar una linea para que muestre la información del sabado y domingo ya que esta programada
de tal forma que toma la fecha de hoy.
*/

USE Shell

GO

--Aqui se agrega la columna donde va a estar la fecha

ALTER TABLE [reporte_17-04-2021_py] ADD Fecha_Precios DATE NULL

GO

/*
Se coloca la fecha en la columna que se creo
*******************************************************************************************************************************
			    EN ESTA LINEA SE TIENE QUE COLOCAR LA FECHA QUE TIENE EL REPORTE (MUY IMPORTANTE)
*******************************************************************************************************************************
*/

UPDATE [reporte_17-04-2021_py] SET Fecha_Precios = '2021-04-17'

GO

--Se limpia el producto tomando en cuenta la primera inicial para determinar que producto es

UPDATE [reporte_17-04-2021_py] SET SubProducto = 'Diesel'
		WHERE SubProducto LIKE ('D%') 
		GO
UPDATE [reporte_17-04-2021_py] SET SubProducto = 'Premium'
		WHERE SubProducto LIKE ('P%')
		GO
UPDATE [reporte_17-04-2021_py] SET SubProducto = 'Regular'
		WHERE SubProducto LIKE ('R%')

GO

--Se limpia la marca para que quede agrupada en un solo nombre 

UPDATE [reporte_17-04-2021_py] SET Marca = 'Pemex'
		WHERE Marca LIKE '%Pemex%' OR Marca LIKE '%Sin Marca%' OR Marca LIKE 'Comborsa%' OR Marca LIKE 'F %'
		   OR Marca LIKE '%Carroil%' OR Marca LIKE '%RedCo%' OR Marca LIKE '%Primero%' OR Marca like '%o plus%' OR Marca like '%NULL%'
GO

UPDATE [reporte_17-04-2021_py] SET Marca = 'Oxxo Gas' WHERE Nombre = 'SERVICIOS GASOLINEROS DE MEXICO, S.A. DE C.V.'

GO

UPDATE [reporte_17-04-2021_py] SET Marca = 'Shell'
		WHERE Marca LIKE '%Shell%'

GO

UPDATE [reporte_17-04-2021_py] SET Marca = 'Repsol'
		WHERE Marca LIKE '%Repsol%'

GO

UPDATE [reporte_17-04-2021_py] SET Marca = 'BP'
		WHERE Marca LIKE '%BP%'

GO

UPDATE [reporte_17-04-2021_py] SET Marca = 'Mobil'
		WHERE Marca LIKE '%Mobil%'

GO

UPDATE [reporte_17-04-2021_py] SET Marca = 'Arco'
		WHERE Marca LIKE '%ARCO%'

GO

UPDATE [reporte_17-04-2021_py] SET Marca = 'Black Gold'
		WHERE Marca LIKE '%Black Gold%'

GO

UPDATE [reporte_17-04-2021_py] SET Marca = 'Total'
		WHERE Marca LIKE 'A-%' OR Marca IN ('Total-advanced','Total-excellium')

GO

UPDATE [reporte_17-04-2021_py] SET Marca = 'Chevron'
		WHERE Marca LIKE '%Chevron%'

GO

UPDATE [reporte_17-04-2021_py] SET Marca = 'Gulf'
		WHERE Marca LIKE '%Gulf%'

GO

UPDATE [reporte_17-04-2021_py] SET Marca = 'La Gas'
		WHERE Marca LIKE '%Techpro%'

GO

UPDATE [reporte_17-04-2021_py] SET Marca = 'G500'
		WHERE Marca LIKE '%G-%'


GO

UPDATE [reporte_17-04-2021_py] SET Marca = 'Sunoco'
		WHERE Marca LIKE '%Sunoco%'


GO

UPDATE [reporte_17-04-2021_py] SET Marca = 'Windstar'
		WHERE Marca LIKE '%Windstar%'


GO

UPDATE [reporte_17-04-2021_py] SET Marca = 'GGR'
		WHERE Marca LIKE '%GGR%'


GO

UPDATE [reporte_17-04-2021_py] SET Marca = 'Masterfuel'
		WHERE Marca LIKE '%Masterfuel%'


GO

UPDATE [reporte_17-04-2021_py] SET Marca = 'Free Energy'
		WHERE Marca LIKE '%Free Energy%'


GO

UPDATE [reporte_17-04-2021_py] SET Marca = 'Gaxo'
		WHERE Marca LIKE '%Gaxo%'


GO

--Se agrega la información limpia a la Base de Datos Maestra de los precios

INSERT INTO Street_Prices (Brand, Permit, Site_Name, Site_Address, Product,Sub_Product, Price, Application_Date, Site_State, Site_Municipality, Prices_Date)
	   SELECT Marca, Numero, Nombre, Direccion,Producto, SubProducto, PrecioVigente,FechaAplicacion, EntidadFederativaId, MunicipioId, Fecha_Precios
	   FROM [reporte_17-04-2021_py]

GO

--Muestra la información que se va a colocar en el Dashboard
--NOTA : Colocar la fecha adecuada para obtener la info correcta

SELECT Prices_Date, Brand, Permit, Site_Name, 
	   Sub_Product = 
	   (
		   CASE 
			WHEN Sub_Product = 'Diesel' THEN 'Diesel' 
			WHEN Sub_Product = 'Premium' THEN 'Premium'
			WHEN Sub_Product = 'Regular' THEN 'Regular'
			ELSE NULL
			END 
		), Price
		 FROM Street_Prices WHERE Prices_Date = CONVERT(DATE, GETDATE()) --between '2019-08-03' and '2019-08-04' --
		 AND Site_State IN ('Aguascalientes', 'Ciudad de México', 'Guanajuato', 'Jalisco', 
		 'México', 'Puebla','Querétaro', 'Hidalgo', 'Coahuila de Zaragoza', 'Colima','Nayarit')
		 AND Price > 17;

		 