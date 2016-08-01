/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package framework.rasdaman;

import java.text.MessageFormat;
import util.Pair;

/**
 * Create/drop types in rasdaman.
 *
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class RasdamanTypeManager {
    
    private final RasdamanSystem systemController;

    public RasdamanTypeManager(RasdamanSystem systemController) {
        this.systemController = systemController;
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
    
    public String createBaseType(String... baseTypes) {
        String baseTypeName = getBaseTypeName(baseTypes);
        if (baseTypes.length > 0) {
            String baseTypeDefinition = MessageFormat.format("create type {0} as ({1})", baseTypeName, getBands(baseTypes));
            systemController.executeRasqlQuery(baseTypeDefinition);
        }
        return baseTypeName;
    }
    
    public String getMddTypeName(int noOfDimensions, String baseTypeName) {
        String mddTypeName = MessageFormat.format("B_MDD_{0}_{1}", baseTypeName, noOfDimensions);
        return mddTypeName;
    }
    
    public String createMddType(int noOfDimensions, String baseTypeName) {
        String mddTypeName = getMddTypeName(noOfDimensions, baseTypeName);
        String mddTypeDefinition = MessageFormat.format("create type {0} as {1} mdarray [ {2} ]", mddTypeName, baseTypeName, getDimNames(noOfDimensions));
        systemController.executeRasqlQuery(mddTypeDefinition);
        return mddTypeName;
    }
    
    public String getSetTypeName(int noOfDimensions, String baseTypeName) {
        String setTypeName = MessageFormat.format("B_SET_{0}_{1}", baseTypeName, noOfDimensions);
        return setTypeName;
    }
    
    public String createSetType(int noOfDimensions, String baseTypeName, String mddTypeName) {
        String setTypeName = getSetTypeName(noOfDimensions, baseTypeName);
        String setTypeDefinition = MessageFormat.format("create type {0} as set ({1})", setTypeName, mddTypeName);
        systemController.executeRasqlQuery(setTypeDefinition);
        return setTypeName;
    }
    
    public Pair<String, String> createType(int noOfDimensions, String... baseTypes) throws Exception {
        String baseTypeName = createBaseType(baseTypes);
        String mddTypeName = createMddType(noOfDimensions, baseTypeName);
        String setTypeName = createSetType(noOfDimensions, baseTypeName, mddTypeName);
        return Pair.of(mddTypeName, setTypeName);
    }

    public void deleteTypes(String... typeNames) {
        for (String typeName : typeNames) {
            if (systemController.executeRasqlQuery("drop type " + typeName) != 0) {
                System.out.printf("Failed to delete set type");
            }
        }
    }
}
