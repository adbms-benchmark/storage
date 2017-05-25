/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package system.rasdaman;

import util.Pair;

import java.text.MessageFormat;
import java.util.Date;

/**
 * Create/drop types in rasdaman.
 *
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class RasdamanTypeManager {
    
    private final RasdamanQueryExecutor queryExecutor;
    private static int SET_TYPE = 0;
    private static int MDD_TYPE = 0;

    public RasdamanTypeManager(RasdamanQueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }
    
    private String getDimNames(int noOfDimensions) {
        StringBuilder dimNames = new StringBuilder("");
        for (int i = 0; i < noOfDimensions; i++) {
            if (i > 0) {
                dimNames.append(",");
            }
            dimNames.append("d" + i);
        }
        return dimNames.toString();
    }
    
    private String getBands(String... baseTypes) {
        StringBuilder bands = new StringBuilder("");
        for (int i = 0; i < baseTypes.length; i++) {
            if (i > 0) {
                bands.append(",");
            }
            bands.append("att").append(i).append(" ").append(baseTypes[i]);
        }
        return bands.toString();
    }
    
    public String getBaseTypeName(String... baseTypes) {
        String baseTypeName = baseTypes[0];
        if (baseTypes.length > 0) {
            baseTypeName = MessageFormat.format("{0}{1}", baseTypeName, baseTypes.length);
        }
        return baseTypeName;
    }
    
    public String createBaseType(String... baseTypes) throws Exception {
        String baseTypeName = getBaseTypeName(baseTypes);
        if (baseTypes.length > 0) {
            String baseTypeDefinition = MessageFormat.format("create type {0} as ({1})", baseTypeName, getBands(baseTypes));
            queryExecutor.executeTimedQuery(baseTypeDefinition);
        }
        return baseTypeName;
    }
    
    public String getMddTypeName(int noOfDimensions, String baseTypeName) {
        String mddTypeName = MessageFormat.format("B_MDD_{0}_{1}", baseTypeName, noOfDimensions);
        return mddTypeName;
    }
    
    public String createMddType(int noOfDimensions, String baseTypeName) throws Exception {
        String mddTypeName = getMddTypeName(noOfDimensions, baseTypeName);
        String mddTypeDefinition = MessageFormat.format("create type {0} as {1} mdarray [ {2} ]", mddTypeName, baseTypeName, getDimNames(noOfDimensions));
        queryExecutor.executeTimedQuery(mddTypeDefinition);
        return mddTypeName;
    }

    public String getSetTypeName(int noOfDimensions, String baseTypeName) {
        String setTypeName = MessageFormat.format("B_SET_{0}_{1}", baseTypeName, noOfDimensions);
        return setTypeName;
    }
    
    public String createSetType(int noOfDimensions, String baseTypeName, String mddTypeName) throws Exception {
        String setTypeName = getSetTypeName(noOfDimensions, baseTypeName);
        String setTypeDefinition = MessageFormat.format("create type {0} as set ({1})", setTypeName, mddTypeName);
        queryExecutor.executeTimedQuery(setTypeDefinition);
        return setTypeName;
    }
    
    public Pair<String, String> createType(int noOfDimensions, String... baseTypes) throws Exception {
        String baseTypeName = createBaseType(baseTypes);
        String mddTypeName = createMddType(noOfDimensions, baseTypeName);
        String setTypeName = createSetType(noOfDimensions, baseTypeName, mddTypeName);
        return Pair.of(mddTypeName, setTypeName);
    }




//    public String createOperationsBaseType(String... baseTypes) throws Exception {
//        String baseTypeName = getBaseTypeName(baseTypes);
//        if (baseTypes.length > 0) {
//            String baseTypeDefinition = MessageFormat.format("create type {0} as ({1})", baseTypeName, getBands(baseTypes));
//            queryExecutor.executeTimedQuery(baseTypeDefinition);
//        }
//        return baseTypeName;
//    }
    public String getOperationsMddTypeName(int noOfDimensions, String baseTypeName) {
        Date d = new Date();
        String mddTypeName = String.format("B_MDD_%s_%d_%d", baseTypeName, noOfDimensions, d.getTime());
        return mddTypeName;
    }

    public String createOperationsMddType(int noOfDimensions, String baseTypeName) throws Exception {
        String mddTypeName = getOperationsMddTypeName(noOfDimensions, baseTypeName);
        String mddTypeDefinition = MessageFormat.format("create type {0} as {1} mdarray [ {2} ]", mddTypeName, baseTypeName, getDimNames(noOfDimensions));
        queryExecutor.executeTimedQuery(mddTypeDefinition);
        return mddTypeName;
    }

    public String getOperationsSetTypeName(int noOfDimensions, String baseTypeName) {
        Date d = new Date();
        String setTypeName = String.format("B_SET_%s_%d_%s", baseTypeName, noOfDimensions, d.getTime());
//        SET_TYPE++;
        return setTypeName;
    }

    public String createOperationsSetType(int noOfDimensions, String baseTypeName, String mddTypeName) throws Exception {
        String setTypeName = getOperationsSetTypeName(noOfDimensions, baseTypeName);
        String setTypeDefinition = MessageFormat.format("create type {0} as set ({1})", setTypeName, mddTypeName);
        queryExecutor.executeTimedQuery(setTypeDefinition);
        return setTypeName;
    }

    public Pair<String, String> createOperationsType(int noOfDimensions, String... baseTypes) throws Exception {
        String baseTypeName;
        if(baseTypes.length > 1) {
            baseTypeName = createBaseType(baseTypes);
        }  else {
            baseTypeName = baseTypes[0];
        }
        String mddTypeName = createOperationsMddType(noOfDimensions, baseTypeName);
        String setTypeName = createOperationsSetType(noOfDimensions, baseTypeName, mddTypeName);
        return Pair.of(mddTypeName, setTypeName);
    }

    public void deleteTypes(String... typeNames) throws Exception {
        for (String typeName : typeNames) {
            if (queryExecutor.executeTimedQuery("drop type " + typeName) != 0) {
                System.out.printf("Failed to delete set type");
            }
        }
    }
}
