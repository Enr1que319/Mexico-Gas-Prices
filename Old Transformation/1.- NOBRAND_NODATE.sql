/*
Este Query sirve para colocar marca a los archivos nuevos ya que estos no vienen con la marca apartir del 5 de Junio del 2019,
por ello se toma en cuenta ese último reporte para igualar la marca según el permiso

NOTA: Utilizar Cntl + F para cambiar el nombre del reporte en todas las lineas 

*/

USE Shell

GO

UPDATE [reporte_17-04-2021_py] SET [reporte_17-04-2021_py].MARCA = [reporte_05-06-2019_py].MARCA FROM [reporte_05-06-2019_py]
WHERE [reporte_17-04-2021_py].Numero = [reporte_05-06-2019_py].Numero

GO

UPDATE [reporte_17-04-2021_py] SET [reporte_17-04-2021_py].MARCA = brand FROM Street_Prices
WHERE [reporte_17-04-2021_py].Numero = Permit AND [reporte_17-04-2021_py].MARCA is null

GO

UPDATE [reporte_17-04-2021_py] SET [reporte_17-04-2021_py].MARCA = 'Shell' FROM Permit_Competitors
 WHERE [reporte_17-04-2021_py].Numero = Permit_Competitors.Permit

GO

UPDATE [reporte_17-04-2021_py] SET Marca = 'Pemex' WHERE Marca IS NULL


 
 
	  
